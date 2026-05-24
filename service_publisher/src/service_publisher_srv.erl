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
%% `timer_ref`: Ref returned by timer:send_interval/2, kept for cancellation on shutdown.
-record(state, {
	conn_opts  :: [{atom(), term()}],
    conn_pid   :: pid() | undefined,
    topic      :: binary() | undefined,
	sensor     :: binary() | undefined,
    timer_ref  :: timer:tref() | undefined  % ref returned by timer:send_interval/2
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
	io:format("Publisher Worker Started~n"),
	self() ! connect,
    {ok, #state{
		conn_opts = [{host, "mosquitto"}, {port, 1883}, {clientid, client_id_from_hostname()}]
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
handle_info(connect, _State = #state{conn_opts = Opts}) ->
    {ok, Pid} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(Pid),

    % Wall-clock timer: fires every 1ms regardless of how long publish_tick takes.
    % Equivalent to Pekko's .throttle(1000, 1.second).
    {ok, TRef} = timer:send_interval(1, publish_tick),

    OsHostname = case os:getenv("HOSTNAME") of false -> "unknown"; H -> H end,
	SensorBin = iolist_to_binary(io_lib:format("sensor~s", [OsHostname])),
	Topic = iolist_to_binary(["sensors/", SensorBin]),

	{noreply, #state{
		conn_opts = Opts,
		conn_pid  = Pid,
		topic     = Topic,
		sensor    = SensorBin,
		timer_ref = TRef
	}};

%% @private
%% @doc Handles periodic data publication.
handle_info(publish_tick,
			State = #state{conn_pid = Pid,
						   topic = Topic,
						   sensor = SensorBin}) ->
	RandomValue = integer_to_binary(rand:uniform(10)),
	Timestamp = calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}, {offset, "Z"}]),
	TimestampBinary = list_to_binary(Timestamp),
	JsonMap = #{<<"device_name">> => SensorBin, <<"timestamp">> => TimestampBinary, <<"value">> => RandomValue},
	Json = json:encode(JsonMap),
    emqtt:publish(Pid, Topic, Json, 0),
    {noreply, State};

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% --- Internal helpers ---

client_id_from_hostname() ->
    OsHostname = case os:getenv("HOSTNAME") of
        false -> <<"erlang_service">>;
        H      -> iolist_to_binary(["erlang_service_", H])
    end.

%% @private
%% @doc Disconnects from MQTT broker on termination.
terminate(_Reason, #state{conn_pid = Pid, timer_ref = TRef}) ->
    timer:cancel(TRef),
    if is_pid(Pid) -> emqtt:disconnect(Pid);
       true -> ok
    end,
    ok.
