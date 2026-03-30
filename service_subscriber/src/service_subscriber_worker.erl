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
handle_cast(Msg, #state{sum = Sum}) ->
	% Decode the JSON payload
	#{} = Data = json:decode(Msg),
	<<_/binary>> = DeviceName = maps:get(<<"device_name">>, Data),
	<<_/binary>> = Timestamp = maps:get(<<"timestamp">>, Data),
	<<_/binary>> = BinaryValue = maps:get(<<"value">>, Data),
	
    % Convert the value to an integer for calculation
	Value = binary_to_integer(BinaryValue),
	
	% Calculate the new total sum
	TotalSum = case Sum of undefined -> Value; _ -> Value + Sum end,
	
    io:format("[Worker ~p] Received message. Sensor: ~s, Sum: ~p, Last Timestamp: ~s~n",
			  [self(), DeviceName, TotalSum, Timestamp]),
    
    {noreply, #state{
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
