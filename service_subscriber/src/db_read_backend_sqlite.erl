%% @doc SQLite backend implementing the db_read_backend behaviour. One connection per reader, opened
%% through db_backend_sqlite:open/0 so a reader cannot be configured apart from the writers.
%%
%% esqlite blocks, which is what read/1's contract wants. Under WAL a read does not wait for the
%% writer, but it does need a dirty IO scheduler the writers also use; see finding R in audit.md.
%%
%% Reads DB_PATH and DB_INIT_DIR through db_backend_sqlite; their defaults live in
%% docker-compose.sqlite.yaml.
-module(db_read_backend_sqlite).
-behaviour(db_read_backend).

-include_lib("kernel/include/logger.hrl").

-export([init/1, read/1, terminate/1]).

%% State of one reader's connection.
%% `conn`: The esqlite connection owned by this reader alone.
%% `stmt`: The read query, prepared once.
-record(sqr_state, {
    conn :: esqlite3:esqlite3(),
    stmt :: esqlite3:esqlite3_stmt()
}).

%% The SQLite spelling of the read group's query. Timestamp holds epoch microseconds, so the window
%% bound is computed in that unit. Byte-identical to SQLiteReadTarget.scala; the matching rule is per
%% backend.
-define(READ_SQL,
        "SELECT avg(Value), count(*) FROM Data "
        "WHERE Timestamp > CAST(unixepoch('subsec') * 1000000 AS INTEGER) - 5000000").

%% Opens this reader's connection and prepares the read it will reuse.
init(Index) ->
    Conn = db_backend_sqlite:open(),
    {ok, Stmt} = esqlite3:prepare(Conn, ?READ_SQL, [persistent]),
    ?LOG_INFO("SQLite read backend reader ~p opened", [Index]),
    {ok, #sqr_state{conn = Conn, stmt = Stmt}}.

%% Runs the read to completion and discards the row. Reset first for the same reason as
%% db_backend_sqlite:step/2. A failed read is reported so the reader leaves it uncounted.
read(#sqr_state{stmt = Stmt} = State) ->
    _ = esqlite3:reset(Stmt),
    case drain(Stmt) of
        ok              -> {ok, State};
        {error, Reason} -> {error, Reason, State}
    end.

%% Closes the connection when the reader shuts down.
terminate(#sqr_state{conn = Conn}) ->
    _ = esqlite3:close(Conn),
    ok.

%% --- Internal helpers ---

%% Steps until '$done'. Not esqlite3:fetchall/1, which has no clause for '$busy' and would crash the
%% reader instead of reporting a failed read.
drain(Stmt) ->
    case esqlite3:step(Stmt) of
        '$done'                -> ok;
        Row when is_list(Row)  -> drain(Stmt);
        '$busy'                -> {error, busy};
        {error, _} = Error     -> Error
    end.
