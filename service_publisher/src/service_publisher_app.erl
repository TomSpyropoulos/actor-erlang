-module(service_publisher_app).
-behaviour(application).
-behaviour(gen_server).

%% Application callbacks
-export([start/2, stop/1]).

%% API
-export([start_link/0, publish/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-record(state, {conn_pid}).

start(_Type, _Args) ->
    % Start the gen_server and return an application-compatible result
    case start_link() of
        {ok, Pid} -> {ok, Pid};
        {ok, Pid, State} -> {ok, Pid, State};
        {error, Reason} -> {error, Reason}
    end.

stop(_State) ->
    ok.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

publish(Topic, Payload) ->
    gen_server:call(?MODULE, {publish, Topic, Payload}),
    logger:info("Published message to topic ~p: ~p", [Topic, Payload]),
	io:format("TEST~n").

init([]) ->
    Opts = [{host, "mosquitto"}, {port, 1883}, {clientid, <<"erlang_service">>}],
    {ok, Pid} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(Pid),
	spawn(fun() -> publisher_loop(Pid) end),
    {ok, #state{conn_pid = Pid}}.

publisher_loop(Pid) ->
    emqtt:publish(Pid, <<"hello">>, <<"world">>, 0),
	io:format("TEST~n"),
    timer:sleep(1000),
    publisher_loop(Pid).

handle_call({publish, Topic, Payload}, _From, State = #state{conn_pid = Pid}) ->
    Res = emqtt:publish(Pid, Topic, Payload, 0),
    {reply, Res, State}.

handle_cast(_Msg, State) -> {noreply, State}.

terminate(_Reason, #state{conn_pid = Pid}) ->
    emqtt:disconnect(Pid),
    ok.
