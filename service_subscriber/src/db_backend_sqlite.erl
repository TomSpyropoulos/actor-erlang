%% @doc SQLite backend implementing the db_backend behaviour. SQLite is a library, not a server, so
%% each pool worker opens its own connection to one shared file through the esqlite NIF. Buffering
%% and the batch triggers belong to the dispatcher (service_subscriber_db).
%%
%% The only backend that writes inline and returns {sync, ...}: an esqlite handle is not safe to use
%% from several processes, so db_backend_mysql's spawned helper is not an option. Every write holds
%% db_backend_sqlite_lock. Read findings Q through S in audit.md before comparing pool_* or reads_*.
%%
%% Reads DB_PATH and DB_INIT_DIR; their defaults live in docker-compose.sqlite.yaml.
-module(db_backend_sqlite).
-behaviour(db_backend).

-include_lib("kernel/include/logger.hrl").

-export([init/2, insert/5, insert_batch/2, insert_status/3, handle_result/2, terminate/1]).
-export([child_specs/0, open/0]).

%% State of one pool worker's connection.
%% `conn`: The esqlite connection owned by this worker alone.
%% `insert`: The single-row INSERT, stepped once per row on both write paths.
%% `status`: The sensor_status INSERT.
-record(sq_state, {
    conn   :: esqlite3:esqlite3(),
    insert :: esqlite3:esqlite3_stmt(),
    status :: esqlite3:esqlite3_stmt()
}).

-define(INSERT_SQL, "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES (?, ?, ?)").
-define(STATUS_SQL, "INSERT INTO sensor_status (DeviceName, Status) VALUES (?, ?)").

%% connection.sql first, so busy_timeout is in force before init.sql contends for the schema lock.
-define(SETUP_FILES, ["connection.sql", "init.sql"]).

%% --- API Functions ---

%% Opens a connection and applies both setup files under the write lock; finding R in audit.md says
%% why setup needs it. Exported so db_read_backend_sqlite cannot open a connection any other way.
%% Crashes on any error, so a bad path or schema fails at init. Mirrors SQLiteBackend.open().
open() ->
    Path = os:getenv("DB_PATH",     "/var/lib/sqlite/epu.db"),
    Dir  = os:getenv("DB_INIT_DIR", "/sqlite/init"),
    {ok, Conn} = esqlite3:open(Path),
    locked(fun() ->
        lists:foreach(
            fun(File) ->
                [ok = esqlite3:exec(Conn, Sql) || Sql <- statements(filename:join(Dir, File))]
            end,
            ?SETUP_FILES)
    end),
    Conn.

%% --- db_backend Callbacks ---

%% The write lock every pool worker shares, started ahead of the pool.
child_specs() ->
    [#{id      => db_backend_sqlite_lock,
       start   => {db_backend_sqlite_lock, start_link, []},
       restart => permanent,
       shutdown => 5000,
       type    => worker,
       modules => [db_backend_sqlite_lock]}].

%% Opens this worker's connection and prepares both statements. Opts is ignored: a batch steps the
%% single-row statement inside one transaction, so neither batch_size nor batch_enabled changes what
%% needs preparing.
init(Index, _Opts) ->
    Conn = open(),
    {ok, Insert} = esqlite3:prepare(Conn, ?INSERT_SQL, [persistent]),
    {ok, Status} = esqlite3:prepare(Conn, ?STATUS_SQL, [persistent]),
    ?LOG_INFO("SQLite backend worker ~p opened", [Index]),
    {ok, #sq_state{conn = Conn, insert = Insert, status = Status}}.

%% Writes one row in autocommit. The write has already returned, so latency is measured here and
%% reported through {sync, ...}; a failed write reports nothing committed.
insert(#sq_state{insert = Stmt} = State, DeviceName, Value, MsgTs, ProcStart) ->
    Rows = [{DeviceName, Value, MsgTs, ProcStart}],
    {sync, committed(locked(fun() -> write_rows(Stmt, Rows) end), Rows), State}.

%% Writes a flushed buffer as one transaction, so it costs one fsync. Under the lock nobody else is
%% writing, so IMMEDIATE only makes a stray writer fail at BEGIN rather than mid-batch. Mirrors
%% SQLiteBatchTarget.scala.
insert_batch(#sq_state{conn = Conn, insert = Stmt} = State, Rows) ->
    Result = locked(fun() ->
        case esqlite3:exec(Conn, "BEGIN IMMEDIATE") of
            ok         -> finish(Conn, write_rows(Stmt, Rows));
            BeginError -> BeginError
        end
    end),
    {sync, committed(Result, Rows), State}.

%% Writes a status row inline and drops the outcome, matching the other backends, which never track
%% status acks.
insert_status(#sq_state{status = Stmt} = State, DeviceName, Status) ->
    _ = locked(fun() -> step(Stmt, [DeviceName, Status]) end),
    {ok, State}.

%% Every write is acknowledged by its own return, so no message ever belongs to this backend.
handle_result(_Msg, State) ->
    {no_match, State}.

%% Closes the connection when the pool worker shuts down.
terminate(#sq_state{conn = Conn}) ->
    _ = esqlite3:close(Conn),
    ok.

%% --- Internal helpers ---

%% Splits a setup file into statements: whole-line `--` comments dropped, the rest split on
%% semicolons. Both files say so in their headers. Must stay equivalent to
%% SQLiteBackend.statements.
statements(File) ->
    {ok, Bin} = file:read_file(File),
    Lines = [L || L <- string:split(Bin, "\n", all),
                  string:prefix(string:trim(L, leading), "--") =:= nomatch],
    Sql = iolist_to_binary(lists:join("\n", Lines)),
    [S || Part <- string:split(Sql, ";", all), S <- [string:trim(Part)], S =/= <<>>].

%% Runs Write while holding the lock, releasing it even if Write throws.
locked(Write) ->
    ok = db_backend_sqlite_lock:acquire(),
    try Write()
    after db_backend_sqlite_lock:release()
    end.

%% Commits a batch whose rows all stepped, and rolls back otherwise. A failed COMMIT rolls back too,
%% or the transaction would stay open and every later BEGIN on this connection would fail.
finish(Conn, ok) ->
    case esqlite3:exec(Conn, "COMMIT") of
        ok          -> ok;
        CommitError -> _ = esqlite3:exec(Conn, "ROLLBACK"), CommitError
    end;
finish(Conn, WriteError) ->
    _ = esqlite3:exec(Conn, "ROLLBACK"),
    WriteError.

%% Steps the insert once per row, stopping at the first failure.
write_rows(_Stmt, []) ->
    ok;
write_rows(Stmt, [{Name, Value, MsgTs, _ProcStart} | Rest]) ->
    case step(Stmt, [Name, Value, MsgTs]) of
        ok    -> write_rows(Stmt, Rest);
        Error -> Error
    end.

%% Binds and runs a write statement. Reset first: esqlite resets only after '$done', so a statement
%% that failed last time would refuse the bind. '$busy' means busy_timeout ran out.
step(Stmt, Params) ->
    _ = esqlite3:reset(Stmt),
    case esqlite3:bind(Stmt, Params) of
        ok ->
            case esqlite3:step(Stmt) of
                '$done'            -> ok;
                '$busy'            -> {error, busy};
                {error, _} = Error -> Error
            end;
        Error ->
            Error
    end.

%% Turns a write outcome into the latency pairs the dispatcher records. An empty list is a failed
%% write: acking it as a commit would overstate committed_s exactly when the lock starts timing out.
committed(ok, Rows) ->
    latencies_for(Rows);
committed(Error, Rows) ->
    logger:warning("db write failed, ~p rows not committed: ~p", [length(Rows), Error]),
    [].

%% Builds one {E2EUs, SubToDbUs} pair per row. The clock is read once per write, so every row in a
%% batch shares one commit timestamp.
latencies_for(Rows) ->
    Now = os:system_time(microsecond),
    [{max(0, Now - MsgTs), max(0, Now - ProcStart)} || {_, _, MsgTs, ProcStart} <- Rows].
