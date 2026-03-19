-module(service_publisher_srv).
-behaviour(gen_server).

%% API
-export([start_link/0, publish/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    conn_pid :: pid() | undefined,
    topic    :: binary(),
    interval :: integer()
}).

-define(DEFAULT_INTERVAL, 1000). % 1 second

%% --- API Functions ---

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

publish(Topic, Payload) ->
    gen_server:call(?MODULE, {publish, Topic, Payload}).

%% --- gen_server Callbacks ---

init([]) ->
    % 1. Define configuration
    Opts = [{host, "mosquitto"}, {port, 1883}, {clientid, <<"erlang_service">>}],

	io:format("Worker Started~n"),    
    % 2. Connect to MQTT
    {ok, Pid} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(Pid),
    
    % 3. Schedule the first periodic publish (instead of a background loop)
    erlang:send_after(?DEFAULT_INTERVAL, self(), publish_tick),
    
    {ok, #state{
        conn_pid = Pid,
        topic = <<"hello">>,
        interval = ?DEFAULT_INTERVAL
    }}.

handle_call({publish, Topic, Payload}, _From, State = #state{conn_pid = Pid}) ->
    % Manual publish request
    Res = emqtt:publish(Pid, Topic, Payload, 0),
    {reply, Res, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(publish_tick, State = #state{conn_pid = Pid, topic = Topic, interval = Interval}) ->
    % This handles the periodic message
    emqtt:publish(Pid, Topic, <<"world">>, 0),
	io:format("Published Message~n"),
    % Schedule the next tick
    erlang:send_after(Interval, self(), publish_tick),
    
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn_pid = Pid}) ->
    if is_pid(Pid) -> emqtt:disconnect(Pid);
       true -> ok
    end,
    ok.
