-module(service_publisher_srv).
-behaviour(gen_server).

%% API
-export([start_link/0, publish/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("kernel/include/logger.hrl").

%% State of the publisher server.
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
    timer_ref    :: timer:tref() | undefined,
    padding_size :: non_neg_integer() | undefined
}).

%% --- API Functions ---

%% Starts the publisher server and registers it locally.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Manually publishes a message to a specific topic.
publish(Topic, Payload) ->
    gen_server:call(?MODULE, {publish, Topic, Payload}).

%% --- gen_server Callbacks ---

%% Initializes the server state and triggers the connection process.
init([]) ->
    ?LOG_INFO("Publisher Worker Started"),
    self() ! connect,
    {ok, #state{
        conn_opts = [{host, "mosquitto"}, {port, 1883}, {clientid, client_id_from_hostname()}]
    }}.

%% Handles manual publish calls via the emqtt client.
handle_call({publish, Topic, Payload}, _From, State = #state{conn_pid = Pid}) ->
    Res = emqtt:publish(Pid, Topic, Payload, 0),
    {reply, Res, State}.

%% No casts used; satisfy the callback contract.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% Establishes the MQTT connection, starts the publish timer, and derives the topic/sensor identity.
handle_info(connect, _State = #state{conn_opts = Opts}) ->
    {ok, Pid} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(Pid),

    %% Wall-clock timer: fires every 1ms regardless of how long publish_tick takes.
    %% Equivalent to Pekko's .throttle(1000, 1.second).
    {ok, TRef} = timer:send_interval(1, publish_tick),

    OsHostname = hostname("unknown"),
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

%% Publishes one JSON sensor reading per tick, optionally padded for payload-size benchmarks.
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

%% Discards unrecognised messages to keep the gen_server running cleanly.
handle_info(_Info, State) ->
    {noreply, State}.

%% --- Internal helpers ---

%% Reads the HOSTNAME env var, returning Default when it is unset.
hostname(Default) ->
    case os:getenv("HOSTNAME") of
        false -> Default;
        H     -> H
    end.

%% Builds the MQTT client id from HOSTNAME, or a stable default when unset.
client_id_from_hostname() ->
    case hostname(undefined) of
        undefined -> <<"erlang_service">>;
        H         -> iolist_to_binary(["erlang_service_", H])
    end.

%% Cancels the publish timer and disconnects from the MQTT broker on shutdown.
terminate(_Reason, #state{conn_pid = Pid, timer_ref = TRef}) ->
    timer:cancel(TRef),
    if is_pid(Pid) -> emqtt:disconnect(Pid);
       true -> ok
    end,
    ok.
