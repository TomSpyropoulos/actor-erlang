-module(service_subscriber_worker).
-behaviour(gen_server).

%% public API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% @doc The state of a sensor worker actor.
%% `sum`: The running sum of all sensor values received.
%% `lastTimestamp`: The timestamp of the last received message.
-record(state, {
	sum :: integer() | undefined,
	lastTimestamp :: binary() | undefined,
    db_pid :: pid()
}).

%% --- API Functions ---

%% @doc Starts a new worker for a specific sensor topic.
start_link(DB) ->
    gen_server:start_link(?MODULE, [DB], []).


%% --- gen_server Callbacks ---

%% @private
init([DB]) ->
	{ok, #state{db_pid = DB}}.

%% @private
%% @doc Handles incoming sensor data (as JSON) forwarded from the MQTT subscriber.
%% Updates the internal state with the new value and timestamp.
handle_cast(Msg, #state{sum = Sum, db_pid = DB} = State) ->
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
    epgsql:equery(DB, "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES ($1, $2, $3)", [DeviceName, Value, ErlTimestamp]),
	
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
