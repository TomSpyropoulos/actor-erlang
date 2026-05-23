-module(service_subscriber_worker).
-behaviour(gen_server).

%% public API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("kernel/include/logger.hrl").

%% @doc The state of a sensor worker actor.
%% `topic`: The MQTT topic this worker is handling.
%% `sum`: The running sum of all sensor values received.
%% `lastSeen`: Unix timestamp (seconds) when the last message was received locally.
%% `lastStatus`: The last reported status ('ALIVE' or 'MISSING').
-record(state, {
	topic           :: binary(),
	sum             :: integer() | undefined,
	lastSeen        :: integer() | undefined,
	lastStatus      :: binary() | undefined
}).

%% --- API Functions ---

%% @doc Starts a new worker for a specific sensor topic.
start_link(Topic) ->
    gen_server:start_link(?MODULE, [Topic], []).


%% --- gen_server Callbacks ---

%% @private
init([Topic]) ->
    ets:insert(service_subscriber_workers, {Topic, self()}),
	{ok, #state{topic = Topic}}.

%% @private
%% @doc Handles incoming sensor data (as JSON) forwarded from the MQTT subscriber.
%% Updates the internal state with the new value and timestamp.
handle_cast(Msg, #state{topic = Topic, sum = Sum} = State) ->
	% Decode the JSON payload
	#{} = Data = json:decode(Msg),
	<<_/binary>> = DeviceName = maps:get(<<"device_name">>, Data),
	<<_/binary>> = Timestamp = maps:get(<<"timestamp">>, Data),
	<<_/binary>> = BinaryValue = maps:get(<<"value">>, Data),

    % Convert the value to an integer for calculation
	Value = binary_to_integer(BinaryValue),

    % Convert RFC3339 binary to Erlang datetime tuple for epgsql
    % TIMESTAMPTZ expects {{Year, Month, Day}, {Hour, Minute, Second}} where Second can be a float
    {ErlTimestamp, ST} = fast_parse_timestamp(Timestamp),

    % Insert into TimescaleDB
    % Table: Data (DeviceName TEXT, Value INTEGER, Timestamp TIMESTAMPTZ)
    service_subscriber_db:insert(DeviceName, Value, ErlTimestamp),

    %% --- Prometheus Metrics Recording ---
    %% We need to calculate the end-to-end latency of the message.
    %% This is done by comparing the timestamp embedded in the JSON payload (created by the publisher)
    %% with the current system time.
    %%
    %% 1. `os:system_time(microsecond)` gives us the current time in microseconds.
    %% 2. `ST` is the payload timestamp, already parsed into microseconds earlier in this function.
    Now = os:system_time(microsecond),
    RawLatencyUs = Now - ST,

    %% 3. Clock drift between the publisher and subscriber containers could potentially
    %%    result in a negative latency. We cap the minimum latency at 0 microseconds.
    LatencyUs = max(0, RawLatencyUs),

    %% 4. The Erlang Prometheus client (`prometheus.erl`) has a built-in time unit conversion feature.
    %%    If a metric's name ends in a duration unit (like `_milliseconds` or `_seconds`),
    %%    the library expects the observed value to be in Erlang's *native* time unit,
    %%    and it automatically converts it to the requested suffix unit before reporting.
    %%    Therefore, we must convert our microsecond value into native time units here.
    NativeLatency = erlang:convert_time_unit(LatencyUs, microsecond, native),

    %% Increment the total request counter and observe the latency for our quantile summary.
    service_subscriber_metrics:inc_requests(),
    service_subscriber_metrics:observe_latency(NativeLatency),

	% Calculate the new total sum
	TotalSum = case Sum of undefined -> Value; _ -> Value + Sum end,

    {noreply, State#state{
				 sum = TotalSum,
				 lastSeen = os:system_time(second)
				}}.

%% @private
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% @private
handle_info(heartbeat, #state{topic = Topic, lastSeen = LastSeen, lastStatus = LastStatus} = State) ->
    DeviceName = extract_device_name(Topic),
    Now = os:system_time(second),

    {NewStatus, ShouldInsert} = case LastSeen of
        undefined ->
            {<<"MISSING">>, LastStatus =/= <<"MISSING">>};
        _ ->
            Diff = Now - LastSeen,
            Status = case Diff > 1 of true -> <<"MISSING">>; false -> <<"ALIVE">> end,
            {Status, Status =/= LastStatus}
    end,

    case ShouldInsert of
        true ->
            ?LOG_INFO("[Worker ~p] Sensor ~s is now ~s", [self(), DeviceName, NewStatus]),
            service_subscriber_db:insert_status(DeviceName, NewStatus);
        false ->
            ok
    end,

    {noreply, State#state{lastStatus = NewStatus}};

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% --- Internal helpers ---

extract_device_name(Topic) ->
    case binary:split(Topic, <<"/">>) of
        [_, Name] -> Name;
        [Name]    -> Name
    end.

%% @private
terminate(_Reason, #state{topic = Topic}) ->
    ets:delete(service_subscriber_workers, Topic),
    ok.

fast_parse_timestamp(<<Y1,Y2,Y3,Y4, $-, Mo1,Mo2, $-, D1,D2, $T, H1,H2, $:, Mi1,Mi2, $:, S1,S2, $., Ms1,Ms2,Ms3, $Z>>) ->
    Year  = (Y1 - $0) * 1000 + (Y2 - $0) * 100 + (Y3 - $0) * 10 + (Y4 - $0),
    Month = (Mo1 - $0) * 10 + (Mo2 - $0),
    Day   = (D1 - $0) * 10 + (D2 - $0),
    Hour  = (H1 - $0) * 10 + (H2 - $0),
    Min   = (Mi1 - $0) * 10 + (Mi2 - $0),
    Sec   = (S1 - $0) * 10 + (S2 - $0),
    Ms    = (Ms1 - $0) * 100 + (Ms2 - $0) * 10 + (Ms3 - $0),
    ErlTimestamp = {{Year, Month, Day}, {Hour, Min, Sec + Ms / 1000.0}},
    GregorianSecs = calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Min, Sec}}),
    UnixSecs = GregorianSecs - 62167219200,
    ST = UnixSecs * 1000000 + Ms * 1000,
    {ErlTimestamp, ST};
fast_parse_timestamp(Timestamp) ->
    ST = calendar:rfc3339_to_system_time(binary_to_list(Timestamp), [{unit, microsecond}]),
    Secs = ST div 1000000,
    Micro = ST rem 1000000,
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(Secs, second),
    ErlTimestamp = {{Y, Mo, D}, {H, Mi, S + Micro / 1000000.0}},
    {ErlTimestamp, ST}.
