%% @doc TimescaleDB (PostgreSQL) backend implementing the db_read_backend behaviour.
%%
%% One instance is held per reader. The read statement is parsed once at init/1
%% so a read costs no parse round-trip, matching how db_backend_timescaledb
%% pre-parses its writes.
%%
%% Executed synchronously via epgsql rather than epgsqla, unlike every write in
%% this service: there is no ack to correlate here, and the reader needs the
%% elapsed time of the call itself to pace the next one.
%%
%% Connection parameters are read from environment variables at startup:
%%   DB_HOST        (default: timescaledb)
%%   DB_USER        (default: postgres)
%%   DB_PASSWORD    (default: postgres)
%%   DB_NAME        (default: epu)
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

%% The query the whole read group is built on. Fixed text and no parameters: an aggregate over a
%% bounded recent window, so the rows it scans stay roughly constant as the table grows and read
%% latency does not drift upward with elapsed run time the way an unbounded scan would. No device
%% filter, so the reader needs no knowledge of which topics exist. Byte-identical to the query in
%% TimescaleReadTarget.scala -- if the two arms ever issue different SQL the group compares query
%% plans instead of runtimes, so keep them in sync. The rule is per backend: db_read_backend_mysql
%% and MySQLReadTarget.scala must match each other, not this pair.
-define(READ_SQL,
        "SELECT avg(Value), count(*) FROM Data WHERE Timestamp > now() - interval '5 seconds'").

%% Opens a PostgreSQL connection and pre-parses the read statement this reader will reuse.
init(Index) ->
    Host   = os:getenv("DB_HOST",     "timescaledb"),
    Port   = list_to_integer(os:getenv("DB_PORT", "5432")),
    User   = os:getenv("DB_USER",     "postgres"),
    Pass   = os:getenv("DB_PASSWORD", "postgres"),
    DBName = os:getenv("DB_NAME",     "epu"),
    %% Port is passed explicitly rather than left to epgsql's 5432 default so this arm reads the
    %% same five connection keys as DbConfig in the Scala arm, which is what lets one
    %% docker-compose.<backend>.yaml configure both repos identically.
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
%% the reader can log it and leave it uncounted, rather than inflating the read rate with queries
%% that never returned an answer.
read(#tsr_state{db_pid = DB, read_stmt = Stmt} = State) ->
    case epgsql:prepared_query(DB, Stmt, []) of
        {error, Reason} -> {error, Reason, State};
        _Result         -> {ok, State}
    end.

%% Closes the PostgreSQL connection cleanly when the reader shuts down.
terminate(#tsr_state{db_pid = DB}) ->
    epgsql:close(DB),
    ok.
