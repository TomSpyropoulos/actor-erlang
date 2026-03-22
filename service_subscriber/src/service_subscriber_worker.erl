-module(service_subscriber_worker).
-behaviour(gen_server).

%% public API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
	% sum :: integer() | undefined
}).

%% --- API Functions ---

start_link() ->
    gen_server:start_link(?MODULE, [], []).


%% --- gen_server Callbacks ---

init([]) ->
	{ok, #state{}}.

handle_cast(Msg, State) ->
	io:format("Received message: ~s~n", [Msg]),
    {noreply, State}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% Handle incoming publish messages - support a few common shapes
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.
