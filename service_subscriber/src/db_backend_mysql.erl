%% @doc MySQL backend implementing the db_backend behaviour.
%%
%% One instance is held per pool worker. Prepared statements are prepared once at
%% init/2 to avoid a parse round-trip on every write.
%%
%% This module does not buffer and does not own the batch triggers: the dispatcher
%% (service_subscriber_db) does, and calls insert/5 or insert_batch/2 accordingly.
%%
%% Unlike epgsql, mysql-otp has no async API -- mysql:execute/3 is a gen_server:call.
%% Both write paths therefore hand the blocking call to a short-lived spawned process
%% that messages the ack back, so this backend still returns {async, State} and its
%% acks still arrive through handle_result/2. That keeps the Erlang arm's execution
%% model the same for both backends, so a timescaledb-vs-mysql difference is the
%% database rather than a change of driver model.
%%
%% What the spawn does NOT recover is wire-level concurrency: the MySQL protocol has
%% no pipelining, so one connection carries one query at a time and every helper
%% queues at the connection process. In-flight writes are therefore capped at
%% DB_POOL_SIZE here where epgsql can hold several per connection. See finding G in
%% audit.md -- pool_* results are not comparable across the two backends.
%%
%% Connection parameters are read from environment variables at startup:
%%   DB_HOST        (default: mysql)
%%   DB_PORT        (default: 3306)
%%   DB_USER        (default: root)
%%   DB_PASSWORD    (default: mysql)
%%   DB_NAME        (default: epu)
-module(db_backend_mysql).
-behaviour(db_backend).

-include_lib("kernel/include/logger.hrl").

-export([init/2, insert/5, insert_batch/2, insert_status/3, handle_result/2, terminate/1]).

-record(my_state, {
    conn       :: pid(),
    %% Number of rows the pre-built batch statement binds. A flush shorter than this
    %% (a BATCH_TIMEOUT_MS flush on a partly filled buffer) cannot use it, because a
    %% multi-row VALUES list has fixed arity.
    batch_size :: pos_integer(),
    %% Internal correlation map: our own Ref -> {MsgTimestampUs, ProcStartUs}
    %% or {batch, Rows} for batch acks
    pending    :: #{reference() => {integer(), integer()} | {batch, list()}}
}).

%% Statement names are fixed atoms, not per-index ones as in db_backend_timescaledb: each worker owns
%% its own connection, so the names never collide, and fixed atoms keep this module off the dynamic
%% atom-creation path entirely.
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
    %% The same five connection keys DbConfig reads in the Scala arm, which is what lets one
    %% docker-compose.mysql.yaml configure both repos identically.
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
%% before the buffer filled) needs its own, since a VALUES list has fixed arity where the
%% TimescaleDB arm's array parameters do not. Mirrors MySQLBatchTarget.scala -- keep the two in sync.
insert_batch(#my_state{conn = Conn, batch_size = BatchSize, pending = P} = State, Rows) ->
    %% One O(n) pass for the count on top of the one that builds the params; at the swept batch
    %% sizes that is cheaper than threading a counter through the dispatcher's contract.
    N      = length(Rows),
    Params = batch_params(Rows),
    Ref = dispatch(fun() ->
        case N =:= BatchSize of
            true  -> mysql:execute(Conn, ?BATCH_STMT, Params);
            false -> mysql:query(Conn, batch_sql(N), Params)
        end
    end),
    {async, State#my_state{pending = P#{Ref => {batch, Rows}}}}.

%% Spawns a status write and drops its ack on the floor, matching the TimescaleDB backend, which
%% fires the query async and never correlates the result. No ref is registered, so nothing reaches
%% handle_result/2 for it.
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

%% Runs Fun in a short-lived process and posts its result back tagged with a fresh ref, which is what
%% lets a driver with no async API still satisfy the behaviour's {async, State} contract.
%%
%% spawn_link, not spawn: mysql:execute/3 returns {error, _} for a query the server rejected, so the
%% only way the helper crashes is the connection process dying, which leaves this worker unusable.
%% Linking makes that kill the worker for the supervisor to restart, rather than silently dropping
%% the ack and leaking its ref in `pending` forever.
%%
%% Acks may arrive out of the order they were dispatched. Nothing depends on that order: each ack
%% carries the rows it covered, and latencies are stamped when it lands.
dispatch(Fun) ->
    Ref    = make_ref(),
    Parent = self(),
    _ = spawn_link(fun() -> Parent ! {mysql_ack, Ref, Fun()} end),
    Ref.

%% Builds the multi-row INSERT text for exactly N rows. Fixed arity is why the size has to be known:
%% the statement prepared at init only fits a full batch.
batch_sql(N) ->
    Tuples = lists:join(", ", lists:duplicate(N, "(?, ?, ?)")),
    lists:flatten(["INSERT INTO Data (DeviceName, Value, Timestamp) VALUES ", Tuples]).

%% Flattens the buffer into one positional parameter list matching batch_sql/1's placeholders.
%% The DB-shaped timestamp is derived here, inside the pass that already builds the list, so nothing
%% driver-specific has to travel down from the worker.
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

%% Converts epoch microseconds to the {{Y,Mo,D},{H,Mi,SecFloat}} tuple mysql-otp binds to DATETIME(6).
%% Fractional seconds carry the microseconds, so the wire format's full precision reaches the column.
%% UTC, matching Clock.toLocalDateTimeUtc in the Scala arm, so a reading lands at the same instant in
%% both arms against a column type that stores no offset.
%%
%% Duplicated from db_backend_timescaledb:micros_to_datetime/1 rather than shared: the tuple shape
%% coincides but the target column types do not, and the two must be free to diverge if either
%% driver's binding changes. Keep them in step -- if one gains a fix, check the other.
micros_to_datetime(Us) ->
    Secs  = Us div 1000000,
    Micro = Us rem 1000000,
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(Secs, second),
    {{Y, Mo, D}, {H, Mi, S + Micro / 1000000.0}}.
