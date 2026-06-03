%% @doc TimescaleDB (PostgreSQL) backend implementing the db_backend behaviour.
%%
%% One instance is held per pool worker. Prepared statements are parsed once
%% at init/1 to avoid a parse round-trip on every write. Writes are dispatched
%% asynchronously via epgsqla; the DB ack arrives as a {Pid, Ref, Result}
%% message which handle_result/2 claims by matching the worker's own db_pid —
%% this ensures only acks from this specific connection are consumed here.
%%
%% Connection parameters are read from environment variables at startup:
%%   DB_HOST     (default: timescaledb)
%%   DB_USER     (default: postgres)
%%   DB_PASSWORD (default: postgres)
%%   DB_NAME     (default: epu)
-module(db_backend_timescaledb).
-behaviour(db_backend).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("kernel/include/logger.hrl").

-export([init/1, insert/6, insert_status/3, handle_result/2, terminate/1]).

%% db_pid is stored so handle_result/2 can reject acks from other connections.
%% Statements are pre-parsed at init to skip the parse phase on each write.
-record(ts_state, {
    db_pid             :: pid(),
    insert_stmt        :: #statement{},
    insert_status_stmt :: #statement{}
}).

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
    {ok, #ts_state{
        db_pid             = DB,
        insert_stmt        = InsertStmt,
        insert_status_stmt = InsertStatusStmt
    }}.

insert(#ts_state{db_pid = DB, insert_stmt = Stmt} = State,
       DeviceName, Value, ErlTimestamp, _MsgTs, _ProcStart) ->
    #statement{types = Types} = Stmt,
    TypedParams = lists:zip(Types, [DeviceName, Value, ErlTimestamp]),
    Ref = epgsqla:prepared_query(DB, Stmt, TypedParams),
    {async, Ref, State}.

insert_status(#ts_state{db_pid = DB, insert_status_stmt = Stmt} = State,
              DeviceName, Status) ->
    #statement{types = Types} = Stmt,
    TypedParams = lists:zip(Types, [DeviceName, Status]),
    epgsqla:prepared_query(DB, Stmt, TypedParams),
    {ok, State}.

%% The DB pid appears in both argument positions: Erlang unification rejects
%% acks from any other connection without an explicit guard.
handle_result({DB, Ref, _Result}, #ts_state{db_pid = DB} = State) when is_reference(Ref) ->
    {match, Ref, State};
handle_result(_Msg, State) ->
    {no_match, State}.

terminate(#ts_state{db_pid = DB}) ->
    epgsql:close(DB),
    ok.
