-module(service_subscriber_db).
-behaviour(gen_server).

-export([start_link/1, insert/3, insert_status/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(POOL_SIZE, 5).

-record(state, {
    db_pid :: pid() | undefined
}).

start_link(Index) ->
    Name = worker_name(Index),
    gen_server:start_link({local, Name}, ?MODULE, [Index], []).

worker_name(Index) ->
    list_to_atom("service_subscriber_db_" ++ integer_to_list(Index)).

insert(DeviceName, Value, ErlTimestamp) ->
    Index = erlang:phash2(DeviceName, ?POOL_SIZE) + 1,
    gen_server:cast(worker_name(Index), {insert, DeviceName, Value, ErlTimestamp}).

insert_status(DeviceName, Status) ->
    Index = erlang:phash2(DeviceName, ?POOL_SIZE) + 1,
    gen_server:cast(worker_name(Index), {insert_status, DeviceName, Status}).

init([Index]) ->
    {ok, DB} = epgsql:connect("timescaledb", "postgres", "postgres", #{
        database => "epu",
        timeout => 5000
    }),
    io:format("Connected to TimescaleDB worker ~p~n", [Index]),
    {ok, #state{db_pid = DB}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({insert, DeviceName, Value, ErlTimestamp}, #state{db_pid = DB} = State) ->
    epgsql:equery(DB, "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES ($1, $2, $3)",
                  [DeviceName, Value, ErlTimestamp]),
    {noreply, State};

handle_cast({insert_status, DeviceName, Status}, #state{db_pid = DB} = State) ->
    epgsql:equery(DB, "INSERT INTO sensor_status (DeviceName, Status) VALUES ($1, $2)",
                  [DeviceName, Status]),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db_pid = DB}) ->
    case is_pid(DB) of
        true  -> epgsql:close(DB);
        false -> ok
    end,
    ok.