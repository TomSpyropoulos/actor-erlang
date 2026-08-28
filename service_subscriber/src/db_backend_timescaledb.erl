%% @doc TimescaleDB (PostgreSQL) backend implementing the db_backend behaviour.
%%
%% One instance is held per pool worker. Prepared statements are parsed once
%% at init/2 to avoid a parse round-trip on every write.
%%
%% This module does not buffer and does not own the batch triggers: the
%% dispatcher (service_subscriber_db) does, and calls insert/5 or insert_batch/2
%% accordingly. All that lives here is how a write is executed against epgsql
%% and how its ack is correlated back to the rows it covered.
%%
%% Both write paths are dispatched asynchronously via epgsqla. The DB ack
%% arrives as a {Pid, Ref, Result} message which handle_result/2 claims by
%% matching the worker's own db_pid. Latencies are computed internally and
%% returned to the dispatcher as {match, [{E2E, Sub}], NewState} -- one pair for
%% a single insert, one per row for a batch.
%%
%% Connection parameters are read from environment variables at startup:
%%   DB_HOST        (default: timescaledb)
%%   DB_USER        (default: postgres)
%%   DB_PASSWORD    (default: postgres)
%%   DB_NAME        (default: epu)
-module(db_backend_timescaledb).
-behaviour(db_backend).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("kernel/include/logger.hrl").

-export([init/2, insert/5, insert_batch/2, insert_status/3, handle_result/2, terminate/1]).

-record(ts_state, {
    db_pid             :: pid(),
    insert_stmt        :: #statement{},
    insert_status_stmt :: #statement{},
    batch_insert_stmt  :: #statement{} | undefined,
    %% Internal correlation map: epgsql Ref -> {MsgTimestampUs, ProcStartUs}
    %% or {batch, Rows} for batch acks
    pending            :: #{reference() => {integer(), integer()} | {batch, list()}}
}).

%% Opens a PostgreSQL connection and pre-parses the prepared statements this worker will use.
%% The batch statement is prepared only when the dispatcher says batching is on, so a non-batch
%% run does not pay an extra parse per worker at startup.
init(Index, #{batch_enabled := BatchEnabled}) ->
    Host   = os:getenv("DB_HOST",     "timescaledb"),
    User   = os:getenv("DB_USER",     "postgres"),
    Pass   = os:getenv("DB_PASSWORD", "postgres"),
    DBName = os:getenv("DB_NAME",     "epu"),
    {ok, DB} = epgsql:connect(Host, User, Pass, #{
        database => DBName,
        timeout  => 5000
    }),
    ?LOG_INFO("TimescaleDB backend worker ~p connected", [Index]),
    {ok, InsertStmt} = epgsql:parse(DB,
        "insert_data_" ++ integer_to_list(Index),
        "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES ($1, $2, $3)",
        []),
    {ok, InsertStatusStmt} = epgsql:parse(DB,
        "insert_status_" ++ integer_to_list(Index),
        "INSERT INTO sensor_status (DeviceName, Status) VALUES ($1, $2)",
        []),

    %% Pre-parse the unnest batch statement once so insert_batch/2 can reuse it
    %% for any batch size without a per-flush parse round-trip.
    BatchInsertStmt = case BatchEnabled of
        true ->
            {ok, S} = epgsql:parse(DB,
                "batch_insert_data_" ++ integer_to_list(Index),
                "INSERT INTO Data (DeviceName, Value, Timestamp) "
                "SELECT unnest($1::text[]), unnest($2::int4[]), unnest($3::timestamptz[])",
                []),
            S;
        false ->
            undefined
    end,

    {ok, #ts_state{
        db_pid             = DB,
        insert_stmt        = InsertStmt,
        insert_status_stmt = InsertStatusStmt,
        batch_insert_stmt  = BatchInsertStmt,
        pending            = #{}
    }}.

%% Sends a single async prepared-query to PostgreSQL and stores the ref for later latency computation.
insert(#ts_state{db_pid = DB, insert_stmt = Stmt, pending = P} = State,
       DeviceName, Value, MsgTs, ProcStart) ->
    #statement{types = Types} = Stmt,
    TypedParams = lists:zip(Types, [DeviceName, Value, micros_to_datetime(MsgTs)]),
    Ref = epgsqla:prepared_query(DB, Stmt, TypedParams),
    {async, State#ts_state{pending = P#{Ref => {MsgTs, ProcStart}}}}.

%% Sends a flushed buffer as a single unnest INSERT and registers the batch ref for latency tracking.
%% Three array parameters keep the SQL text fixed at any batch size, which is what lets the statement
%% be parsed once at init. Mirrors the Scala TimescaleBatchTarget -- keep the two in sync.
insert_batch(#ts_state{db_pid = DB, batch_insert_stmt = Stmt, pending = P} = State, Rows) ->
    Names      = [N  || {N, _, _, _} <- Rows],
    Values     = [V  || {_, V, _, _} <- Rows],
    %% The DB-shaped timestamp is derived here, inside the pass that already builds the arrays,
    %% so nothing epgsql-specific has to travel down from the worker.
    Timestamps = [micros_to_datetime(MsgTs) || {_, _, MsgTs, _} <- Rows],
    #statement{types = Types} = Stmt,
    TypedParams = lists:zip(Types, [Names, Values, Timestamps]),
    Ref = epgsqla:prepared_query(DB, Stmt, TypedParams),
    {async, State#ts_state{pending = P#{Ref => {batch, Rows}}}}.

%% Fires an async prepared query to record a sensor status change; the ack is intentionally ignored.
insert_status(#ts_state{db_pid = DB, insert_status_stmt = Stmt} = State,
              DeviceName, Status) ->
    #statement{types = Types} = Stmt,
    TypedParams = lists:zip(Types, [DeviceName, Status]),
    epgsqla:prepared_query(DB, Stmt, TypedParams),
    {ok, State}.

%% Claims a PostgreSQL ack matched by ref and computes latencies for each row in the pending map.
%% A failed write yields no latencies, so the dispatcher counts nothing as committed: acking an
%% error as a commit overstated committed_s under exactly the saturation it was meant to detect.
handle_result({DB, Ref, Result},
              #ts_state{db_pid = DB, pending = P} = State) when is_reference(Ref) ->
    case maps:take(Ref, P) of
        {Pending, Rest} ->
            %% Take the ref on the error path too — leaving it behind leaks `pending` for the life
            %% of the worker, the same ratcheting failure as the finding-2 timer leak.
            State1 = State#ts_state{pending = Rest},
            case is_error_result(Result) of
                true ->
                    logger:warning("db write failed, rows not committed: ~p", [Result]),
                    {match, [], State1};
                false ->
                    {match, latencies_for(Pending), State1}
            end;
        error ->
            {no_match, State}
    end;

%% Passes through messages not owned by this backend without modifying state.
handle_result(_Msg, State) ->
    {no_match, State}.

%% Closes the PostgreSQL connection cleanly when the pool worker shuts down.
terminate(#ts_state{db_pid = DB}) ->
    epgsql:close(DB),
    ok.

%% --- Internal helpers ---

%% epgsql reports a failed statement as {error, _}; a multi-statement ack arrives as a list, so a
%% single failure anywhere in it disqualifies the whole ack.
is_error_result({error, _}) ->
    true;
is_error_result(Results) when is_list(Results) ->
    lists:any(fun({error, _}) -> true; (_) -> false end, Results);
is_error_result(_) ->
    false.

%% Builds one {E2EUs, SubToDbUs} pair per acknowledged row. The ack clock is read once per ack so
%% every row in a batch shares one flush timestamp.
latencies_for({batch, Rows}) ->
    FlushTime = os:system_time(microsecond),
    [{max(0, FlushTime - MsgTs), max(0, FlushTime - ProcStart)}
     || {_, _, MsgTs, ProcStart} <- Rows];
latencies_for({MsgTs, ProcStart}) ->
    FlushTime = os:system_time(microsecond),
    [{max(0, FlushTime - MsgTs), max(0, FlushTime - ProcStart)}].

%% Converts epoch microseconds to the {{Y,Mo,D},{H,Mi,SecFloat}} tuple epgsql binds to timestamptz.
%% Fractional seconds carry the microseconds, so the wire format's full precision reaches the column.
micros_to_datetime(Us) ->
    Secs  = Us div 1000000,
    Micro = Us rem 1000000,
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(Secs, second),
    {{Y, Mo, D}, {H, Mi, S + Micro / 1000000.0}}.
