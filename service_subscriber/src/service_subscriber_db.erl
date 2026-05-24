%% @doc DB connection pool — one GenServer per connection (pool of 20).
%%
%% Writes are sharded by hashing DeviceName so a given sensor always hits the
%% same worker (preserving write order). Queries are fire-and-forget via
%% epgsqla; results arrive in handle_info and are discarded.
-module(service_subscriber_db).
-behaviour(gen_server).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/1, insert/3, insert_status/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(POOL_SIZE, 20).

-record(state, {
    db_pid             :: pid() | undefined,
    insert_stmt        :: #statement{} | undefined,
    insert_status_stmt :: #statement{} | undefined
}).

start_link(Index) ->
    Name = worker_name(Index),
    gen_server:start_link({local, Name}, ?MODULE, [Index], []).

%% Atom name for pool slot Index, e.g. service_subscriber_db_3.
worker_name(Index) ->
    list_to_atom("service_subscriber_db_" ++ integer_to_list(Index)).

%% @doc Async insert of a sensor data row. Non-blocking for the caller.
insert(DeviceName, Value, ErlTimestamp) ->
    Index = erlang:phash2(DeviceName, ?POOL_SIZE) + 1,
    gen_server:cast(worker_name(Index), {insert, DeviceName, Value, ErlTimestamp}).

%% @doc Async insert of a sensor status change row. Non-blocking for the caller.
insert_status(DeviceName, Status) ->
    Index = erlang:phash2(DeviceName, ?POOL_SIZE) + 1,
    gen_server:cast(worker_name(Index), {insert_status, DeviceName, Status}).

init([Index]) ->
    {ok, DB} = epgsql:connect("timescaledb", "postgres", "postgres", #{
        database => "epu",
        timeout  => 5000
    }),
    ?LOG_INFO("Connected to TimescaleDB worker ~p", [Index]),

    %% Parse statements once at startup to avoid per-query parse round-trips.
    {ok, InsertStmt} = epgsql:parse(DB, "insert_data_" ++ integer_to_list(Index),
                                    "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES ($1, $2, $3)", []),
    {ok, InsertStatusStmt} = epgsql:parse(DB, "insert_status_" ++ integer_to_list(Index),
                                          "INSERT INTO sensor_status (DeviceName, Status) VALUES ($1, $2)", []),
    {ok, #state{
        db_pid             = DB,
        insert_stmt        = InsertStmt,
        insert_status_stmt = InsertStatusStmt
    }}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({insert, DeviceName, Value, ErlTimestamp}, #state{db_pid = DB, insert_stmt = InsertStmt} = State) ->
    #statement{types = Types} = InsertStmt,
    TypedParams = lists:zip(Types, [DeviceName, Value, ErlTimestamp]),
    epgsqla:prepared_query(DB, InsertStmt, TypedParams),
    {noreply, State};

handle_cast({insert_status, DeviceName, Status}, #state{db_pid = DB, insert_status_stmt = InsertStatusStmt} = State) ->
    #statement{types = Types} = InsertStatusStmt,
    TypedParams = lists:zip(Types, [DeviceName, Status]),
    epgsqla:prepared_query(DB, InsertStatusStmt, TypedParams),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Receives async query results from epgsqla; discarded for throughput.
handle_info({DB, Ref, _Result}, #state{db_pid = DB} = State) when is_reference(Ref) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db_pid = DB}) ->
    case is_pid(DB) of
        true  -> epgsql:close(DB);
        false -> ok
    end,
    ok.