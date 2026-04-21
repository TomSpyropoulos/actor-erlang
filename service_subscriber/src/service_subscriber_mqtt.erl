-module(service_subscriber_mqtt).
-behaviour(gen_server).

%% public API
-export([start_link/0, subscribe/2, unsubscribe/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% @doc The state of the subscriber MQTT handler.
%% `conn_opts`: Options used to connect to the MQTT broker.
%% `conn_pid`: The PID of the emqtt client.
%% `topic`: The wildcard topic this subscriber is listening to.
%% `subscribers`: A map of topic strings to the PIDs of worker actors.
-record(state, {
	conn_opts:: [{atom(), term()}],
	conn_pid :: pid() | undefined,
    topic    :: binary() | undefined,
    subscribers :: map()  % maps TopicBinary -> Pid
}).

%% --- API Functions ---

%% @doc Starts the MQTT subscriber server.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Manually register a subscriber PID for a specific topic.
subscribe(Topic, Pid) ->
    gen_server:call(?MODULE, {subscribe, Topic, Pid}).

%% @doc Manually unregister a subscriber PID for a specific topic.
unsubscribe(Topic, Pid) ->
    gen_server:call(?MODULE, {unsubscribe, Topic, Pid}).

%% --- gen_server Callbacks ---

%% @private
%% @doc Initializes the server state and triggers connection.
init([]) ->
	io:format("MQTT Subscriber Started~n"),
	self() ! connect_mqtt,
    {ok, #state{
		conn_opts = [{host, "mosquitto"}, {port, 1883}, {clientid, <<"erlang_subscriber">>}],
        subscribers = #{}
    }}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_call({subscribe, Topic, Pid}, _From, State=#state{subscribers = Subs}) ->
    NewSubs = maps:put(Topic, Pid, Subs),
    {reply, ok, State#state{subscribers = NewSubs}};

%% @private
handle_call({unsubscribe, Topic, Pid}, _From, State=#state{subscribers = Subs}) ->
    case maps:find(Topic, Subs) of
        {ok, ExistingPid} when ExistingPid =:= Pid ->
            NewSubs = maps:remove(Topic, Subs),
            {reply, ok, State#state{subscribers = NewSubs}};
        _ ->
            {reply, ok, State}
    end;

%% @private
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% @private
%% @doc Handles the 'connect' message to establish connection and subscribe to topics.
handle_info(connect_mqtt, #state{conn_opts = Opts} = State) ->
    % start and connect client
    {ok, Pid} = emqtt:start_link(Opts),
    % connect may be synchronous in this client
    _ = (catch emqtt:connect(Pid)),
    Topic = <<"sensors/#">>,

    _ = (catch emqtt:subscribe(Pid, Topic)),
    io:format("Subscribed to ~p~n", [Topic]),
    {noreply, State#state{
        conn_pid = Pid,
        topic = Topic
    }};

%% @private
%% @doc Handle incoming publish messages from the MQTT broker.
handle_info({publish, #{topic := Topic, payload := Payload}}, State) when is_binary(Topic) ->
	spawn_or_forward(Topic, Payload, State).

%% @private
%% @doc Disconnects from MQTT on termination.
terminate(_Reason, #state{conn_pid = MqttPid}) ->
    case is_pid(MqttPid) of
        true  -> emqtt:disconnect(MqttPid);
        false -> ok
    end,
    ok.

%% Internal helpers

%% @private
%% @doc Forwards a payload to an existing worker actor or spawns a new one if necessary.
spawn_or_forward(Topic, Payload, State=#state{subscribers = Subs}) ->
    case maps:find(Topic, Subs) of
        {ok, Pid} when is_pid(Pid) ->
            gen_server:cast(Pid, Payload),
            {noreply, State};
        error ->
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
