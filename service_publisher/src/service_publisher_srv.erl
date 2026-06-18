-module(service_publisher_srv).
-behaviour(gen_server).

%% API
-export([start_link/0, publish/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("kernel/include/logger.hrl").

%% @doc The state of the publisher server.
%% `conn_opts`: Options used to connect to the MQTT broker.
%% `conn_pid`: The PID of the emqtt client.
%% `topic`: The MQTT topic this publisher is sending data to.
%% `sensor`: The unique identifier for this sensor (derived from HOSTNAME).
%% `timer_ref`: Ref returned by timer:send_interval/2, kept for cancellation on shutdown.
%% `padding_size`: Bytes of filler to add to each payload (PAYLOAD_PADDING_BYTES), for payload-size benchmarks.
-record(state, {
	conn_opts    :: [{atom(), term()}],
    conn_pid     :: pid() | undefined,
    topic        :: binary() | undefined,
	sensor       :: binary() | undefined,
    timer_ref    :: timer:tref() | undefined,  % ref returned by timer:send_interval/2
    padding_size :: non_neg_integer() | undefined
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
	?LOG_INFO("Publisher Worker Started"),
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
	PaddingSize = case os:getenv("PAYLOAD_PADDING_BYTES") of
		false -> 0;
		PaddingStr -> list_to_integer(PaddingStr)
	end,

	{noreply, #state{
		conn_opts    = Opts,
		conn_pid     = Pid,
		topic        = Topic,
		sensor       = SensorBin,
		timer_ref    = TRef,
		padding_size = PaddingSize
	}};

%% @private
%% @doc Handles periodic data publication.
handle_info(publish_tick,
			State = #state{conn_pid = Pid,
						   topic = Topic,
						   sensor = SensorBin,
						   padding_size = PaddingSize}) ->
	RandomValue = integer_to_binary(rand:uniform(10)),
	Timestamp = calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}, {offset, "Z"}]),
	TimestampBinary = list_to_binary(Timestamp),
	BaseMap = #{<<"device_name">> => SensorBin, <<"timestamp">> => TimestampBinary, <<"value">> => RandomValue},
	%% Extra filler field for payload-size benchmarks; omitted when PAYLOAD_PADDING_BYTES is unset/0.
	JsonMap = case PaddingSize of
		0 -> BaseMap;
		_ -> BaseMap#{<<"padding">> => binary:copy(<<"x">>, PaddingSize)}
	end,
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
