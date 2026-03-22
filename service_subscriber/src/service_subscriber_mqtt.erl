-module(service_subscriber_mqtt).
-behaviour(gen_server).

%% public API
-export([start_link/0, subscribe/2, unsubscribe/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
	conn_opts:: [{atom(), term()}],
	conn_pid :: pid() | undefined,
    topic    :: binary() | undefined,
    subscribers :: map()  % maps TopicBinary -> Pid
}).

%% --- API Functions ---

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

subscribe(Topic, Pid) ->
    gen_server:call(?MODULE, {subscribe, Topic, Pid}).

unsubscribe(Topic, Pid) ->
    gen_server:call(?MODULE, {unsubscribe, Topic, Pid}).

%% --- gen_server Callbacks ---

init([]) ->
	io:format("MQTT worker started~n"),
	self() ! connect,
    {ok, #state{
		conn_opts = [{host, "mosquitto"}, {port, 1883}, {clientid, <<"erlang_subscriber">>}],
        subscribers = #{}
    }}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call({subscribe, Topic, Pid}, _From, State=#state{subscribers = Subs}) ->
    NewSubs = maps:put(Topic, Pid, Subs),
    {reply, ok, State#state{subscribers = NewSubs}};

handle_call({unsubscribe, Topic, Pid}, _From, State=#state{subscribers = Subs}) ->
    case maps:find(Topic, Subs) of
        {ok, ExistingPid} when ExistingPid =:= Pid ->
            NewSubs = maps:remove(Topic, Subs),
            {reply, ok, State#state{subscribers = NewSubs}};
        _ ->
            {reply, ok, State}
    end;

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% Handle connect message: connect to broker and subscribe to wildcard topic
handle_info(connect, #state{conn_opts = Opts} = State) ->
    % start and connect client
    {ok, Pid} = emqtt:start_link(Opts),
    % connect may be synchronous in this client
    _ = (catch emqtt:connect(Pid)),
    Topic = <<"sensors/#">>,

    _ = (catch emqtt:subscribe(Pid, Topic)),
    io:format("Subscribed to ~p~n", [Topic]),
    {noreply, State#state{
        conn_opts = Opts,
        conn_pid = Pid,
        topic = Topic
    }};

%% Handle incoming publish messages - support a few common shapes
handle_info({publish, #{topic := Topic, payload := Payload}}, State) when is_binary(Topic) ->
	spawn_or_forward(Topic, Payload, State).

terminate(_Reason, #state{conn_pid = Pid}) ->
    if is_pid(Pid) -> emqtt:disconnect(Pid);
       true -> ok
    end,
    ok.

%% Internal helpers

spawn_or_forward(Topic, Payload, State=#state{subscribers = Subs}) ->
    case maps:find(Topic, Subs) of
        {ok, Pid} when is_pid(Pid) ->
            % forward payload to existing worker
			gen_server:cast(Pid, Payload),
            {noreply, State};
        error ->
            % start a new per-topic worker, register it in the map, and forward payload
            case service_subscriber_worker:start_link() of
                {ok, NewPid} ->
                    NewSubs = maps:put(Topic, NewPid, Subs),
					gen_server:cast(NewPid, Payload),
                    {noreply, State#state{subscribers = NewSubs}};
                {error, Reason} ->
                    io:format("Failed to start worker for ~p: ~p~n", [Topic, Reason]),
                    {noreply, State}
            end
    end.
