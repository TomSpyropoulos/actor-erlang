%% @doc MySQL backend implementing the db_read_backend behaviour.
%%
%% One instance is held per reader. The read statement is prepared once at init/1
%% so a read costs no parse round-trip, matching how db_backend_mysql prepares its
%% writes.
%%
%% mysql:execute/3 is a gen_server:call, and here that is exactly what the contract
%% wants: read/1 is specified as blocking, because the reader has nothing else to do
%% while the query runs and needs the elapsed time of the call itself to pace the
%% next one. The spawned-helper indirection db_backend_mysql needs on the write path
%% is therefore deliberately absent from this module.
%%
%% Connection parameters are read from environment variables at startup:
%%   DB_HOST        (default: mysql)
%%   DB_PORT        (default: 3306)
%%   DB_USER        (default: root)
%%   DB_PASSWORD    (default: mysql)
%%   DB_NAME        (default: epu)
-module(db_read_backend_mysql).
-behaviour(db_read_backend).

-include_lib("kernel/include/logger.hrl").

-export([init/1, read/1, terminate/1]).

%% State of one reader's connection.
%% `conn`: The mysql-otp connection owned by this reader alone.
-record(myr_state, {
    conn :: pid()
}).

%% Statement name is a fixed atom, not a per-index one: each reader owns its own connection, so the
%% names never collide, and fixed atoms keep this module off the dynamic atom-creation path.
-define(READ_STMT, read_data).

%% The MySQL spelling of the query the whole read group is built on. Fixed text and no parameters: an
%% aggregate over a bounded recent window, so the rows it scans stay roughly constant as the table
%% grows and read latency does not drift upward with elapsed run time the way an unbounded scan
%% would. That bound is only cheap because mysql/init/init.sql indexes Timestamp -- InnoDB has no
%% equivalent of the chunk exclusion TimescaleDB gets from create_hypertable. No device filter, so
%% the reader needs no knowledge of which topics exist. NOW(6), not NOW(), to match the microsecond
%% resolution of the Postgres now() the TimescaleDB arm uses.
%%
%% Byte-identical to the query in MySQLReadTarget.scala -- if the two arms ever issue different SQL
%% the group compares query plans instead of runtimes, so keep them in sync. The rule is per backend:
%% this pair must match each other, not the TimescaleDB pair.
-define(READ_SQL,
        "SELECT avg(Value), count(*) FROM Data WHERE Timestamp > NOW(6) - INTERVAL 5 SECOND").

%% Opens a MySQL connection and prepares the read statement this reader will reuse.
init(Index) ->
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
    ?LOG_INFO("MySQL read backend reader ~p connected", [Index]),
    {ok, _} = mysql:prepare(Conn, ?READ_STMT, ?READ_SQL),
    {ok, #myr_state{conn = Conn}}.

%% Runs the prepared read to completion and discards the result set. A failed read is reported so
%% the reader can log it and leave it uncounted, rather than inflating the read rate with queries
%% that never returned an answer.
read(#myr_state{conn = Conn} = State) ->
    case mysql:execute(Conn, ?READ_STMT, []) of
        {error, Reason} -> {error, Reason, State};
        _Result         -> {ok, State}
    end.

%% Closes the MySQL connection cleanly when the reader shuts down.
terminate(#myr_state{conn = Conn}) ->
    mysql:stop(Conn),
    ok.
