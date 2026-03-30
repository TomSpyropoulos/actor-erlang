-module(service_publisher_srv).
-behaviour(gen_server).

%% API
-export([start_link/0, publish/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% @doc The state of the publisher server.
%% `conn_opts`: Options used to connect to the MQTT broker.
%% `conn_pid`: The PID of the emqtt client.
%% `topic`: The MQTT topic this publisher is sending data to.
%% `sensor`: The unique identifier for this sensor (derived from HOSTNAME).
%% `interval`: The interval in milliseconds between data publications.
-record(state, {
	conn_opts:: [{atom(), term()}],
    conn_pid :: pid() | undefined,
    topic    :: binary() | undefined,
	sensor	 :: binary() | undefined,
    interval :: integer() | undefined
}).

%% --- API Functions ---

%% @doc Starts the publisher server.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Manually publish a message to a specific topic.
publish(Topic, Payload) ->
    gen_server:call(?MODULE, {publish, Topic, Payload}).

%% --- gen_server Callbacks ---

%% @private
%% @doc Initializes the server state and triggers the connection process.
init([]) ->
	% initialize worker
	io:format("Publisher Worker Started~n"),
	self() ! connect, 
    {ok, #state{
		conn_opts = [{host, "mosquitto"}, {port, 1883}, {clientid, <<"erlang_service">>}],
		interval = 100 % Interval in ms, at which a message will be published
    }}.

%% @private
%% @doc Handles manual publish calls.
handle_call({publish, Topic, Payload}, _From, State = #state{conn_pid = Pid}) ->
    % Manual publish request using the emqtt client
    Res = emqtt:publish(Pid, Topic, Payload, 0),
    {reply, Res, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
%% @doc Handles the 'connect' message to establish MQTT connection.
handle_info(connect, _State = #state{conn_opts = Opts, interval = Interval}) ->
    % Connect to MQTT broker
    {ok, Pid} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(Pid),
	
    % Schedule the first periodic publication
	erlang:send_after(Interval, self(), publish_tick),
	
    % Generate a unique sensor name using the container's HOSTNAME
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

%% @private
%% @doc Handles periodic data publication.
handle_info(publish_tick,
			State = #state{conn_pid = Pid,
						   topic = Topic,
						   sensor = SensorBin,
						   interval = Interval}) ->
    % Generate random sensor data
	RandomValue = integer_to_binary(rand:uniform(10)),
	Timestamp = calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}, {offset, "Z"}]),
	TimestampBinary = list_to_binary(Timestamp),
	% Build a JSON payload
	JsonMap = #{<<"device_name">> => SensorBin, <<"timestamp">> => TimestampBinary, <<"value">> => RandomValue},
	Json = json:encode(JsonMap),
    
    % Publish to MQTT
    emqtt:publish(Pid, Topic, Json, 0),
	io:format("Published message from ~s: ~s~n", [SensorBin, Json]),
    
    % Schedule the next tick
    erlang:send_after(Interval, self(), publish_tick),
    {noreply, State};

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
%% @doc Disconnects from MQTT broker on termination.
terminate(_Reason, #state{conn_pid = Pid}) ->
    if is_pid(Pid) -> emqtt:disconnect(Pid);
       true -> ok
    end,
    ok.
