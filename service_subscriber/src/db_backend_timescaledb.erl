%% @doc TimescaleDB (PostgreSQL) backend implementing the db_backend behaviour.
%%
%% One instance is held per pool worker. Prepared statements are parsed once
%% at init/1 to avoid a parse round-trip on every write.
%%
%% Non-batching mode: each insert is dispatched asynchronously via epgsqla.
%% The DB ack arrives as a {Pid, Ref, Result} message which handle_result/2
%% claims by matching the worker's own db_pid. Latencies are computed
%% internally and returned to the dispatcher as {match, [{E2E, Sub}], NewState}.
%%
%% Batching mode: rows are buffered in state until the batch reaches
%% BATCH_SIZE or a BATCH_TIMEOUT_MS timer fires. On flush a single
%% multi-row INSERT is sent via epgsqla:equery/3. Latencies for all rows
%% in the batch are computed on ack and returned as a list.
%%
%% Connection parameters are read from environment variables at startup:
%%   DB_HOST        (default: timescaledb)
%%   DB_USER        (default: postgres)
%%   DB_PASSWORD    (default: postgres)
%%   DB_NAME        (default: epu)
%%
%% Batching parameters:
%%   BATCH_ENABLED    (default: false)
%%   BATCH_SIZE       (default: 100)
%%   BATCH_TIMEOUT_MS (default: 1000)
-module(db_backend_timescaledb).
-behaviour(db_backend).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("kernel/include/logger.hrl").

-export([init/1, insert/6, insert_status/3, handle_result/2, terminate/1]).

-record(ts_state, {
    db_pid             :: pid(),
    insert_stmt        :: #statement{},
    insert_status_stmt :: #statement{},
    batch_insert_stmt  :: #statement{} | undefined,
    %% Internal correlation map: epgsql Ref -> {MsgTimestampUs, ProcStartUs}
    %% or {batch, Rows} for batch acks
    pending            :: #{reference() => {integer(), integer()} | {batch, list()}},
    %% Batching config
    batch_enabled      :: boolean(),
    batch_size         :: integer(),
    batch_timeout_ms   :: integer(),
    %% Buffer: list of {DeviceName, Value, ErlTimestamp, MsgTs, ProcStart}
    buffer             :: list(),
    timer_ref          :: reference() | undefined,
    self_pid           :: pid()
}).

%% Opens a PostgreSQL connection, pre-parses prepared statements, and configures batching from env vars.
init(Index) ->
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

    BatchEnabled = os:getenv("BATCH_ENABLED", "false") =:= "true",
    BatchSize    = list_to_integer(os:getenv("BATCH_SIZE",       "100")),
    BatchTimeMs  = list_to_integer(os:getenv("BATCH_TIMEOUT_MS", "1000")),
    SelfPid      = self(),

    %% Pre-parse the unnest batch statement once so flush/1 can reuse it
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

    State = #ts_state{
        db_pid             = DB,
        insert_stmt        = InsertStmt,
        insert_status_stmt = InsertStatusStmt,
        batch_insert_stmt  = BatchInsertStmt,
        pending            = #{},
        batch_enabled      = BatchEnabled,
        batch_size         = BatchSize,
        batch_timeout_ms   = BatchTimeMs,
        buffer             = [],
        timer_ref          = undefined,
        self_pid           = SelfPid
    },

    %% Start the first flush timer when batching is on.
    State2 = case BatchEnabled of
        true  -> schedule_timer(State);
        false -> State
    end,
    {ok, State2}.

%% Sends a single async prepared-query to PostgreSQL and stores the ref for later latency computation.
insert(#ts_state{batch_enabled = false,
                 db_pid = DB, insert_stmt = Stmt,
                 pending = P} = State,
       DeviceName, Value, ErlTimestamp, MsgTs, ProcStart) ->
    #statement{types = Types} = Stmt,
    TypedParams = lists:zip(Types, [DeviceName, Value, ErlTimestamp]),
    Ref = epgsqla:prepared_query(DB, Stmt, TypedParams),
    {async, State#ts_state{pending = P#{Ref => {MsgTs, ProcStart}}}};

%% Appends the row to the in-memory buffer and flushes immediately when the batch size threshold is reached.
insert(#ts_state{batch_enabled = true,
                 buffer = Buf, batch_size = BatchSize} = State,
       DeviceName, Value, ErlTimestamp, MsgTs, ProcStart) ->
    Row = {DeviceName, Value, ErlTimestamp, MsgTs, ProcStart},
    NewBuf = [Row | Buf],
    State1 = State#ts_state{buffer = NewBuf},
    if
        length(NewBuf) >= BatchSize ->
            %% Batch full — flush immediately.
            State2 = flush(State1),
            {async, State2};
        length(NewBuf) =:= 1 ->
            %% First row in a new batch — ensure timer is running.
            State2 = schedule_timer(State1),
            {buffered, State2};
        true ->
            {buffered, State1}
    end.

%% Fires an async prepared query to record a sensor status change; the ack is intentionally ignored.
insert_status(#ts_state{db_pid = DB, insert_status_stmt = Stmt} = State,
              DeviceName, Status) ->
    #statement{types = Types} = Stmt,
    TypedParams = lists:zip(Types, [DeviceName, Status]),
    epgsqla:prepared_query(DB, Stmt, TypedParams),
    {ok, State}.

%% Flushes any buffered rows when the timeout fires and reschedules the next flush timer.
handle_result(flush_batch, #ts_state{batch_enabled = true,
                                      buffer = Buf} = State) ->
    State1 = State#ts_state{timer_ref = undefined},
    State2 = case Buf of
        [] -> State1;
        _  -> flush(State1)
    end,
    %% Reschedule for the next interval.
    {no_match, schedule_timer(State2)};

%% Claims a PostgreSQL ack matched by ref and computes latencies for each row in the pending map.
handle_result({DB, Ref, _Result},
              #ts_state{db_pid = DB, pending = P} = State) when is_reference(Ref) ->
    case maps:take(Ref, P) of
        {{batch, Rows}, Rest} ->
            %% Batch ack — compute latency for every row in the batch.
            FlushTime = os:system_time(microsecond),
            Latencies = [{max(0, FlushTime - MsgTs), max(0, FlushTime - ProcStart)}
                         || {_, _, _, MsgTs, ProcStart} <- Rows],
            {match, Latencies, State#ts_state{pending = Rest}};
        {{MsgTs, ProcStart}, Rest} ->
            %% Single-row ack (non-batching mode).
            FlushTime = os:system_time(microsecond),
            Latencies = [{max(0, FlushTime - MsgTs), max(0, FlushTime - ProcStart)}],
            {match, Latencies, State#ts_state{pending = Rest}};
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

%% Sends the entire buffer as a single unnest INSERT and registers the batch ref for latency tracking.
flush(#ts_state{db_pid = DB, batch_insert_stmt = Stmt,
                buffer = Buf, pending = P} = State) ->
    Rows = lists:reverse(Buf),
    Names      = [N  || {N, _, _,  _, _} <- Rows],
    Values     = [V  || {_, V, _,  _, _} <- Rows],
    Timestamps = [Ts || {_, _, Ts, _, _} <- Rows],
    #statement{types = Types} = Stmt,
    TypedParams = lists:zip(Types, [Names, Values, Timestamps]),
    Ref = epgsqla:prepared_query(DB, Stmt, TypedParams),
    State#ts_state{
        buffer    = [],
        timer_ref = undefined,
        pending   = P#{Ref => {batch, Rows}}
    }.

%% Arms a one-shot timer to flush the buffer after batch_timeout_ms if no timer is already running.
schedule_timer(#ts_state{timer_ref = undefined,
                          batch_timeout_ms = Ms,
                          self_pid = Pid} = State) ->
    Ref = erlang:send_after(Ms, Pid, flush_batch),
    State#ts_state{timer_ref = Ref};
%% Returns state unchanged when a timer is already in flight to avoid double-scheduling.
schedule_timer(State) ->
    State.
