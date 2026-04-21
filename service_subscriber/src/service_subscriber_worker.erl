-module(service_subscriber_worker).
-behaviour(gen_server).

%% public API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% @doc The state of a sensor worker actor.
%% `sum`: The running sum of all sensor values received.
%% `lastTimestamp`: The timestamp of the last received message.
-record(state, {
	sum :: integer() | undefined,
	lastTimestamp :: binary() | undefined
}).

%% --- API Functions ---

%% @doc Starts a new worker for a specific sensor topic.
start_link() ->
    gen_server:start_link(?MODULE, [], []).


%% --- gen_server Callbacks ---

%% @private
init([]) ->
	{ok, #state{}}.

%% @private
%% @doc Handles incoming sensor data (as JSON) forwarded from the MQTT subscriber.
%% Updates the internal state with the new value and timestamp.
handle_cast(Msg, #state{sum = Sum} = State) ->
	% Decode the JSON payload
	#{} = Data = json:decode(Msg),
	<<_/binary>> = DeviceName = maps:get(<<"device_name">>, Data),
	<<_/binary>> = Timestamp = maps:get(<<"timestamp">>, Data),
	<<_/binary>> = BinaryValue = maps:get(<<"value">>, Data),
	
    % Convert the value to an integer for calculation
	Value = binary_to_integer(BinaryValue),

    % Convert RFC3339 binary to Erlang datetime tuple for epgsql
    % TIMESTAMPTZ expects {{Year, Month, Day}, {Hour, Minute, Second}} where Second can be a float
    ST = calendar:rfc3339_to_system_time(binary_to_list(Timestamp), [{unit, microsecond}]),
    Secs = ST div 1000000,
    Micro = ST rem 1000000,
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(Secs, second),
    ErlTimestamp = {{Y, Mo, D}, {H, Mi, S + Micro / 1000000.0}},

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
	
    io:format("[Worker ~p] Received message. Sensor: ~s, Sum: ~p, Last Timestamp: ~s~n",
			  [self(), DeviceName, TotalSum, Timestamp]),
    
    {noreply, State#state{
				 sum = TotalSum,
				 lastTimestamp = Timestamp
				}}.

%% @private
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, #state{}) ->
    ok.
