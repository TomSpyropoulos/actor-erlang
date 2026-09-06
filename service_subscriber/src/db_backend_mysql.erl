%% @doc MySQL backend implementing the db_backend behaviour. One instance per pool worker,
%% statements prepared once at init/2. Buffering and the batch triggers belong to the dispatcher
%% (service_subscriber_db); what lives here is how a write is executed and how its ack is
%% correlated back to the rows it covered.
%%
%% mysql-otp has no async API, so both write paths hand the blocking call to a short-lived spawned
%% process that messages the ack back, keeping this backend on the same {async, State} contract as
%% db_backend_timescaledb. It does not recover wire-level concurrency -- read finding G in audit.md
%% before comparing pool_* across the two backends.
%%
%% Reads the five DB_* connection variables; their defaults live in docker-compose.mysql.yaml.
-module(db_backend_mysql).
-behaviour(db_backend).

-include_lib("kernel/include/logger.hrl").

-export([init/2, insert/5, insert_batch/2, insert_status/3, handle_result/2, terminate/1]).

-record(my_state, {
    conn       :: pid(),
    %% Rows the pre-built batch statement binds. A short flush cannot use it: a
    %% multi-row VALUES list has fixed arity.
    batch_size :: pos_integer(),
    %% Internal correlation map: our own Ref -> {MsgTimestampUs, ProcStartUs}
    %% or {batch, Rows} for batch acks
    pending    :: #{reference() => {integer(), integer()} | {batch, list()}}
}).

%% Fixed atoms, not per-index names as in db_backend_timescaledb: each worker owns its own
%% connection, so the names never collide and nothing here creates atoms at runtime.
-define(INSERT_STMT,   insert_data).
-define(STATUS_STMT,   insert_status).
-define(BATCH_STMT,    batch_insert_data).

%% --- db_backend Callbacks ---

%% Opens a MySQL connection and prepares the statements this worker will use. The batch statement is
%% prepared only when the dispatcher says batching is on, so a non-batch run does not pay an extra
%% prepare per worker at startup.
init(Index, #{batch_enabled := BatchEnabled, batch_size := BatchSize}) ->
    Host   = os:getenv("DB_HOST",     "mysql"),
    Port   = list_to_integer(os:getenv("DB_PORT", "3306")),
    User   = os:getenv("DB_USER",     "root"),
    Pass   = os:getenv("DB_PASSWORD", "mysql"),
    DBName = os:getenv("DB_NAME",     "epu"),
    {ok, Conn} = mysql:start_link([
        {host,     Host},
        {port,     Port},
        {user,     User},
        {password, Pass},
        {database, DBName}
    ]),
    ?LOG_INFO("MySQL backend worker ~p connected", [Index]),
    {ok, _} = mysql:prepare(Conn, ?INSERT_STMT,
        "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES (?, ?, ?)"),
    {ok, _} = mysql:prepare(Conn, ?STATUS_STMT,
        "INSERT INTO sensor_status (DeviceName, Status) VALUES (?, ?)"),

    case BatchEnabled of
        true ->
            {ok, _} = mysql:prepare(Conn, ?BATCH_STMT, batch_sql(BatchSize));
        false ->
            ok
    end,

    {ok, #my_state{conn = Conn, batch_size = BatchSize, pending = #{}}}.

%% Hands one row's write to a spawned helper and stores the ref for later latency computation.
insert(#my_state{conn = Conn, pending = P} = State, DeviceName, Value, MsgTs, ProcStart) ->
    Params = [DeviceName, Value, micros_to_datetime(MsgTs)],
    Ref = dispatch(fun() -> mysql:execute(Conn, ?INSERT_STMT, Params) end),
    {async, State#my_state{pending = P#{Ref => {MsgTs, ProcStart}}}}.

%% Hands a flushed buffer to a spawned helper as one multi-row INSERT and registers the batch ref.
%% A full-size flush reuses the statement prepared at init; a short one (BATCH_TIMEOUT_MS fired
%% early) needs its own, since a VALUES list has fixed arity. Mirrors MySQLBatchTarget.scala.
insert_batch(#my_state{conn = Conn, batch_size = BatchSize, pending = P} = State, Rows) ->
    N      = length(Rows),
    Params = batch_params(Rows),
    Ref = dispatch(fun() ->
        case N =:= BatchSize of
            true  -> mysql:execute(Conn, ?BATCH_STMT, Params);
            false -> mysql:query(Conn, batch_sql(N), Params)
        end
    end),
    {async, State#my_state{pending = P#{Ref => {batch, Rows}}}}.

%% Spawns a status write and drops its ack, matching db_backend_timescaledb. No ref is registered,
%% so nothing reaches handle_result/2 for it.
insert_status(#my_state{conn = Conn} = State, DeviceName, Status) ->
    _ = spawn_link(fun() -> _ = mysql:execute(Conn, ?STATUS_STMT, [DeviceName, Status]), ok end),
    {ok, State}.

%% Claims a helper's ack matched by ref and computes latencies for each row in the pending map.
%% A failed write yields no latencies, so the dispatcher counts nothing as committed: acking an
%% error as a commit would overstate committed_s under exactly the saturation it exists to detect.
handle_result({mysql_ack, Ref, Result}, #my_state{pending = P} = State) when is_reference(Ref) ->
    case maps:take(Ref, P) of
        {Pending, Rest} ->
            %% Take the ref on the error path too -- leaving it behind leaks `pending` for the life
            %% of the worker, the same ratcheting failure as the finding-2 timer leak.
            State1 = State#my_state{pending = Rest},
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

%% Closes the MySQL connection cleanly when the pool worker shuts down.
terminate(#my_state{conn = Conn}) ->
    mysql:stop(Conn),
    ok.

%% --- Internal helpers ---

%% Runs Fun in a short-lived process and posts the result back tagged with a fresh ref, which is how
%% a driver with no async API still satisfies the {async, State} contract. Acks may arrive out of
%% dispatch order; nothing depends on it, since each ack carries the rows it covered.
%%
%% spawn_link, not spawn: the only way the helper crashes is the connection process dying, which
%% leaves this worker unusable. Linking kills it for the supervisor to restart, instead of leaking
%% the ref in `pending` forever.
dispatch(Fun) ->
    Ref    = make_ref(),
    Parent = self(),
    _ = spawn_link(fun() -> Parent ! {mysql_ack, Ref, Fun()} end),
    Ref.

%% Builds the multi-row INSERT text for exactly N rows; the statement prepared at init fits only a
%% full batch.
batch_sql(N) ->
    Tuples = lists:join(", ", lists:duplicate(N, "(?, ?, ?)")),
    lists:flatten(["INSERT INTO Data (DeviceName, Value, Timestamp) VALUES ", Tuples]).

%% Flattens the buffer into one positional parameter list matching batch_sql/1's placeholders. The
%% DB-shaped timestamp is derived here so nothing driver-specific travels down from the worker.
batch_params(Rows) ->
    lists:flatmap(
        fun({Name, Value, MsgTs, _ProcStart}) -> [Name, Value, micros_to_datetime(MsgTs)] end,
        Rows).

%% mysql-otp reports a failed statement as {error, _}; a successful INSERT is a bare ok.
is_error_result({error, _}) ->
    true;
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

%% Converts epoch microseconds to the tuple mysql-otp binds to DATETIME(6); fractional seconds carry
%% the microseconds. UTC, matching Clock.toLocalDateTimeUtc, because the column stores no offset.
%% Deliberately not shared with db_backend_timescaledb:micros_to_datetime/1 -- same shape, different
%% target type -- so check the other if this one gains a fix.
micros_to_datetime(Us) ->
    Secs  = Us div 1000000,
    Micro = Us rem 1000000,
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(Secs, second),
    {{Y, Mo, D}, {H, Mi, S + Micro / 1000000.0}}.
