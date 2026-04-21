-module(service_subscriber_db).
-behaviour(gen_server).

-export([start_link/0, insert/3, insert_status/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    db_pid :: pid() | undefined
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

insert(DeviceName, Value, ErlTimestamp) ->
    gen_server:call(?MODULE, {insert, DeviceName, Value, ErlTimestamp}).

insert_status(DeviceName, Status) ->
    gen_server:call(?MODULE, {insert_status, DeviceName, Status}).

init([]) ->
    {ok, DB} = epgsql:connect("timescaledb", "postgres", "postgres", #{
        database => "epu",
        timeout => 5000
    }),
    io:format("Connected to TimescaleDB~n"),
    {ok, #state{db_pid = DB}}.

handle_call({insert, DeviceName, Value, ErlTimestamp}, _From, #state{db_pid = DB} = State) ->
    epgsql:equery(DB, "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES ($1, $2, $3)",
                  [DeviceName, Value, ErlTimestamp]),
    {reply, ok, State};

handle_call({insert_status, DeviceName, Status}, _From, #state{db_pid = DB} = State) ->
    epgsql:equery(DB, "INSERT INTO sensor_status (DeviceName, Status) VALUES ($1, $2)",
                  [DeviceName, Status]),
    {reply, ok, State};

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

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