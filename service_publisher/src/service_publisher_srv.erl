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
    RandomValue = rand:uniform(10),
    %% Same clock and resolution as service_subscriber_worker's arrival stamp; erlang:system_time
    %% is a different (corrected) clock, so don't swap it in. The 6-digit fraction is fixed-width,
    %% which keeps the payload a constant size.
    NowUs = os:system_time(microsecond),
    Timestamp = calendar:system_time_to_rfc3339(NowUs, [{unit, microsecond}, {offset, "Z"}]),
    %% Shared wire format: compact, this field order, unquoted value. Byte-identical to data/2 in
    %% actor-scala/service-publisher/src/main/scala/com/publisher/Main.scala and parsed by
    %% fast_parse_timestamp/1; json:encode would reorder the keys.
    Padding = case PaddingSize of
        0 -> <<>>;
        _ -> [<<",\"padding\":\"">>, binary:copy(<<"x">>, PaddingSize), $"]
    end,
    %% iolist, not iolist_to_binary/1: emqtt takes iodata, so the padding is never copied.
    Json = [<<"{\"device_name\":\"">>, SensorBin,
            <<"\",\"timestamp\":\"">>, Timestamp,
            <<"\",\"value\":">>, integer_to_binary(RandomValue),
            Padding, $}],
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
