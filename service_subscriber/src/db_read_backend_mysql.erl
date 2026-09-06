%% @doc MySQL backend implementing the db_read_backend behaviour. One instance per reader, statement
%% prepared once at init/1.
%%
%% mysql:execute/3 blocks, which is what read/1's contract wants, so the spawned-helper indirection
%% db_backend_mysql needs on the write path is deliberately absent here.
%%
%% Reads the five DB_* connection variables; their defaults live in docker-compose.mysql.yaml.
-module(db_read_backend_mysql).
-behaviour(db_read_backend).

-include_lib("kernel/include/logger.hrl").

-export([init/1, read/1, terminate/1]).

%% State of one reader's connection.
%% `conn`: The mysql-otp connection owned by this reader alone.
-record(myr_state, {
    conn :: pid()
}).

%% Fixed atom, not a per-index name: each reader owns its own connection, so the names never
%% collide and nothing here creates atoms at runtime.
-define(READ_STMT, read_data).

%% The MySQL spelling of the read group's query; see the read-group section of audit.md for why the
%% window is bounded. The bound is only cheap because mysql/init/init.sql indexes Timestamp -- InnoDB
%% has no equivalent of TimescaleDB's chunk exclusion. NOW(6), not NOW(), to match the microsecond
%% resolution of the Postgres now().
%%
%% Byte-identical to MySQLReadTarget.scala, or the group compares query plans instead of runtimes.
%% The rule is per backend: this pair must match each other, not the TimescaleDB pair.
-define(READ_SQL,
        "SELECT avg(Value), count(*) FROM Data WHERE Timestamp > NOW(6) - INTERVAL 5 SECOND").

%% Opens a MySQL connection and prepares the read statement this reader will reuse.
init(Index) ->
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
    ?LOG_INFO("MySQL read backend reader ~p connected", [Index]),
    {ok, _} = mysql:prepare(Conn, ?READ_STMT, ?READ_SQL),
    {ok, #myr_state{conn = Conn}}.

%% Runs the prepared read to completion and discards the result set. A failed read is reported so the
%% reader leaves it uncounted rather than inflating the read rate with queries that never answered.
read(#myr_state{conn = Conn} = State) ->
    case mysql:execute(Conn, ?READ_STMT, []) of
        {error, Reason} -> {error, Reason, State};
        _Result         -> {ok, State}
    end.

%% Closes the MySQL connection cleanly when the reader shuts down.
terminate(#myr_state{conn = Conn}) ->
    mysql:stop(Conn),
    ok.
