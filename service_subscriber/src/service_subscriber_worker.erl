-module(service_subscriber_worker).
-behaviour(gen_server).

%% public API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("kernel/include/logger.hrl").

%% A sensor silent for longer than this at heartbeat time is declared MISSING.
%% Coupled with ?HEARTBEAT_INTERVAL_MS (5000) in service_subscriber_mqtt, which sets
%% how often this check runs — keep the two in sync when tuning liveness sensitivity.
-define(LIVENESS_TIMEOUT_S, 1).

%% Seconds from year 0 (Erlang's gregorian epoch) to 1970-01-01, used to convert
%% calendar:datetime_to_gregorian_seconds/1 output into Unix time.
-define(GREGORIAN_UNIX_OFFSET_S, 62167219200).

%% State of a sensor worker actor.
%% `topic`: The MQTT topic this worker is handling.
%% `sum`: The running sum of all sensor values received.
%% `last_seen`: Unix timestamp (seconds) when the last message was received locally.
%% `last_status`: The last reported status ('ALIVE' or 'MISSING').
-record(state, {
    topic           :: binary(),
    sum             :: integer() | undefined,
    last_seen       :: integer() | undefined,
    last_status     :: binary() | undefined
}).

%% --- API Functions ---

%% Starts an unnamed gen_server worker bound to the given MQTT topic.
start_link(Topic) ->
    gen_server:start_link(?MODULE, [Topic], []).


%% --- gen_server Callbacks ---

%% Registers this worker's PID in the shared ETS table so the MQTT handler can route messages to it.
init([Topic]) ->
    ets:insert(service_subscriber_workers, {Topic, self()}),
    {ok, #state{topic = Topic}}.

%% Decodes a JSON sensor payload, persists it to the DB, and updates running latency metrics.
handle_cast(Msg, #state{topic = Topic, sum = Sum} = State) ->
    #{} = Data = json:decode(Msg),
    <<_/binary>> = DeviceName = maps:get(<<"device_name">>, Data),
    <<_/binary>> = Timestamp = maps:get(<<"timestamp">>, Data),
    %% Bare JSON number on the wire (see service_publisher_srv), so it decodes straight to an integer.
    Value = maps:get(<<"value">>, Data),
    true = is_integer(Value),

    %% Microseconds since the Unix epoch. The DB-shaped form is built in the backend instead, so
    %% nothing epgsql-specific travels through the worker or the dispatcher.
    ST = fast_parse_timestamp(Timestamp),

    %% Capture now before the DB cast so the backend can later compute sub→DB latency.
    ProcessingStartUs = os:system_time(microsecond),

    service_subscriber_db:insert(DeviceName, Value, ST, ProcessingStartUs),

    %% Reuse ProcessingStartUs to avoid a redundant syscall; cap at 0 for clock skew.
    LatencyUs = max(0, ProcessingStartUs - ST),

    %% Native, not milliseconds: prometheus_histogram takes a cheap ets:update_counter path for
    %% integers and an allocating match-spec rebuild for floats.
    NativeLatency = erlang:convert_time_unit(LatencyUs, microsecond, native),

    service_subscriber_metrics:inc_requests(),
    service_subscriber_metrics:observe_latency(NativeLatency),

    TotalSum = case Sum of undefined -> Value; _ -> Value + Sum end,

    {noreply, State#state{
                 sum = TotalSum,
                 %% Reuses ProcessingStartUs like LatencyUs above; the drift is nothing
                 %% against the 1s ?LIVENESS_TIMEOUT_S.
                 last_seen = ProcessingStartUs div 1000000
                }}.

%% No synchronous calls used; satisfy the callback contract.
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% Evaluates sensor liveness based on time since last message and writes a status row when the state changes.
handle_info(heartbeat, #state{topic = Topic, last_seen = LastSeen, last_status = LastStatus} = State) ->
    DeviceName = extract_device_name(Topic),
    Now = os:system_time(second),

    {NewStatus, ShouldInsert} = case LastSeen of
        undefined ->
            {<<"MISSING">>, LastStatus =/= <<"MISSING">>};
        _ ->
            Diff = Now - LastSeen,
            Status = case Diff > ?LIVENESS_TIMEOUT_S of true -> <<"MISSING">>; false -> <<"ALIVE">> end,
            {Status, Status =/= LastStatus}
    end,

    case ShouldInsert of
        true ->
            ?LOG_INFO("[Worker ~p] Sensor ~s is now ~s", [self(), DeviceName, NewStatus]),
            service_subscriber_db:insert_status(DeviceName, NewStatus);
        false ->
            ok
    end,

    %% Always update the gauge so Prometheus always reflects the current state,
    %% even when the status hasn't changed since the last heartbeat.
    service_subscriber_metrics:set_sensor_status(DeviceName, NewStatus),

    {noreply, State#state{last_status = NewStatus}};

%% Discards unrecognised messages to keep the gen_server running cleanly.
handle_info(_Info, State) ->
    {noreply, State}.

%% --- Internal helpers ---

%% Strips the leading topic prefix (e.g. "sensors/") to yield just the device name.
extract_device_name(Topic) ->
    case binary:split(Topic, <<"/">>) of
        [_, Name] -> Name;
        [Name]    -> Name
    end.

%% Removes this worker's ETS entry on shutdown so stale routing entries don't accumulate.
terminate(_Reason, #state{topic = Topic}) ->
    ets:delete(service_subscriber_workers, Topic),
    ok.

%% Fast path for the shared wire format (ISO-8601 UTC, six fractional digits), avoiding calendar
%% overhead. Fixed-width by agreement with service_publisher_srv -- keep the two in sync.
fast_parse_timestamp(<<Y1,Y2,Y3,Y4, $-, Mo1,Mo2, $-, D1,D2, $T, H1,H2, $:, Mi1,Mi2, $:, S1,S2, $., U1,U2,U3,U4,U5,U6, $Z>>) ->
    Year  = (Y1 - $0) * 1000 + (Y2 - $0) * 100 + (Y3 - $0) * 10 + (Y4 - $0),
    Month = (Mo1 - $0) * 10 + (Mo2 - $0),
    Day   = (D1 - $0) * 10 + (D2 - $0),
    Hour  = (H1 - $0) * 10 + (H2 - $0),
    Min   = (Mi1 - $0) * 10 + (Mi2 - $0),
    Sec   = (S1 - $0) * 10 + (S2 - $0),
    Us    = (U1 - $0) * 100000 + (U2 - $0) * 10000 + (U3 - $0) * 1000
          + (U4 - $0) * 100 + (U5 - $0) * 10 + (U6 - $0),
    GregorianSecs = calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Min, Sec}}),
    UnixSecs = GregorianSecs - ?GREGORIAN_UNIX_OFFSET_S,
    UnixSecs * 1000000 + Us;

%% Fallback for anything not matching the tight pattern above (other precisions, non-UTC offset).
fast_parse_timestamp(Timestamp) ->
    calendar:rfc3339_to_system_time(binary_to_list(Timestamp), [{unit, microsecond}]).
