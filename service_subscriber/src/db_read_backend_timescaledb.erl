%% @doc TimescaleDB (PostgreSQL) backend implementing the db_read_backend behaviour. One instance per
%% reader, statement parsed once at init/1.
%%
%% Synchronous epgsql, not epgsqla as every write is: there is no ack to correlate, and read/1's
%% contract is blocking.
%%
%% Reads the five DB_* connection variables; their defaults live in docker-compose.timescaledb.yaml.
-module(db_read_backend_timescaledb).
-behaviour(db_read_backend).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("kernel/include/logger.hrl").

-export([init/1, read/1, terminate/1]).

%% State of one reader's connection.
%% `db_pid`: The epgsql connection owned by this reader alone.
%% `read_stmt`: The pre-parsed read statement, reused for every read.
-record(tsr_state, {
    db_pid    :: pid(),
    read_stmt :: #statement{}
}).

%% The read group's query; see the read-group section of audit.md for why the window is bounded and
%% carries no device filter. Byte-identical to TimescaleReadTarget.scala, or the group compares query
%% plans instead of runtimes. The rule is per backend: db_read_backend_mysql and MySQLReadTarget.scala
%% must match each other, not this pair.
-define(READ_SQL,
        "SELECT avg(Value), count(*) FROM Data WHERE Timestamp > now() - interval '5 seconds'").

%% Opens a PostgreSQL connection and pre-parses the read statement this reader will reuse.
init(Index) ->
    Host   = os:getenv("DB_HOST",     "timescaledb"),
    Port   = list_to_integer(os:getenv("DB_PORT", "5432")),
    User   = os:getenv("DB_USER",     "postgres"),
    Pass   = os:getenv("DB_PASSWORD", "postgres"),
    DBName = os:getenv("DB_NAME",     "epu"),
    %% Port passed explicitly, not left to epgsql's default, so both arms read the same five keys.
    {ok, DB} = epgsql:connect(Host, User, Pass, #{
        database => DBName,
        port     => Port,
        timeout  => 5000
    }),
    ?LOG_INFO("TimescaleDB read backend reader ~p connected", [Index]),
    {ok, ReadStmt} = epgsql:parse(DB,
        "read_data_" ++ integer_to_list(Index),
        ?READ_SQL,
        []),
    {ok, #tsr_state{db_pid = DB, read_stmt = ReadStmt}}.

%% Runs the pre-parsed read to completion and discards the result set. A failed read is reported so
%% the reader leaves it uncounted rather than inflating the read rate with queries that never answered.
read(#tsr_state{db_pid = DB, read_stmt = Stmt} = State) ->
    case epgsql:prepared_query(DB, Stmt, []) of
        {error, Reason} -> {error, Reason, State};
        _Result         -> {ok, State}
    end.

%% Closes the PostgreSQL connection cleanly when the reader shuts down.
terminate(#tsr_state{db_pid = DB}) ->
    epgsql:close(DB),
    ok.
