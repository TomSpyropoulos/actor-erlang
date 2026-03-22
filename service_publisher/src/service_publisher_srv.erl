-module(service_publisher_srv).
-behaviour(gen_server).

%% API
-export([start_link/0, publish/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
	conn_opts:: [{atom(), term()}],
    conn_pid :: pid() | undefined,
    topic    :: binary() | undefined,
	sensor	 :: binary() | undefined,
    interval :: integer() | undefined
}).

%% --- API Functions ---

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

publish(Topic, Payload) ->
    gen_server:call(?MODULE, {publish, Topic, Payload}).

%% --- gen_server Callbacks ---

init([]) ->
	% initialize worker
	io:format("Worker Started~n"),
	self() ! connect, 
    {ok, #state{
		conn_opts = [{host, "mosquitto"}, {port, 1883}, {clientid, <<"erlang_service">>}],
		interval = 100
    }}.

handle_call({publish, Topic, Payload}, _From, State = #state{conn_pid = Pid}) ->
    % Manual publish request
    Res = emqtt:publish(Pid, Topic, Payload, 0),
    {reply, Res, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(connect, _State = #state{conn_opts = Opts, interval = Interval}) ->
    % Connect to MQTT
    {ok, Pid} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(Pid),
	erlang:send_after(Interval, self(), publish_tick),
	OsHostname = case os:getenv("HOSTNAME") of false -> "unknown"; H -> H end,
	SensorBin = iolist_to_binary(io_lib:format("sensor~s", [OsHostname])),
	Topic = iolist_to_binary(["sensors/", SensorBin]),
	{noreply, #state{
		conn_opts = Opts,
		conn_pid = Pid,
		topic = Topic,
		sensor = SensorBin,
		interval = Interval
	}};

handle_info(publish_tick,
			State = #state{conn_pid = Pid,
						   topic = Topic,
						   sensor = SensorBin,
						   interval = Interval}) ->
    % This handles the periodic message
	RandomValue = integer_to_binary(rand:uniform(10)),
	Timestamp = calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}, {offset, "Z"}]),
	% Build a proper JSON string as an iolist and convert to binary
	JsonIolist = [
		<<"{\"device_name\":\"">>, SensorBin,
		<<"\",\"value\":">>, RandomValue,
		<<",\"timestamp\":">>, Timestamp,
		<<"}">>
	],
	JsonStr = iolist_to_binary(JsonIolist),
    emqtt:publish(Pid, Topic, JsonStr, 0),
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
