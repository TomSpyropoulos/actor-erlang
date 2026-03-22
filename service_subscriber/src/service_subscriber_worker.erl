-module(service_subscriber_worker).
-behaviour(gen_server).

%% public API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
	sum :: integer() | undefined,
	lastTimestamp :: binary() | undefined
}).

%% --- API Functions ---

start_link() ->
    gen_server:start_link(?MODULE, [], []).


%% --- gen_server Callbacks ---

init([]) ->
	{ok, #state{}}.

handle_cast(Msg, #state{sum = Sum}) ->
	% assert value types so LSP shuts up. -> Data is always a map in our case.
	#{} = Data = json:decode(Msg),
	<<_/binary>> = DeviceName = maps:get(<<"device_name">>, Data),
	<<_/binary>> = Timestamp = maps:get(<<"timestamp">>, Data),
	<<_/binary>> = BinaryValue = maps:get(<<"value">>, Data),
	Value = binary_to_integer(BinaryValue),
	% TotalSum is value if sum is undefined, or it is the addition
	TotalSum = case Sum of undefined -> Value; _ -> Value + Sum end,
	io:format("Received message, data updated. Sensor: ~s, Sum: ~p, Last Timestamp ~s~n",
			  [DeviceName, TotalSum, Timestamp]),
    {noreply, #state{
				 sum = TotalSum,
				 lastTimestamp = Timestamp
				}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% Handle incoming publish messages - support a few common shapes
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.
