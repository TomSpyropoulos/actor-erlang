-module(service_subscriber_mqtt).
-behaviour(gen_server).

%% public API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% @doc The state of the subscriber MQTT handler.
%% `conn_opts`: Options used to connect to the MQTT broker.
%% `conn_pid`: The PID of the emqtt client.
%% `topic`: The wildcard topic this subscriber is listening to.
-record(state, {
	conn_opts:: [{atom(), term()}],
	conn_pid :: pid() | undefined,
    topic    :: binary() | undefined
}).

%% --- API Functions ---

%% @doc Starts the MQTT subscriber server.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% --- gen_server Callbacks ---

%% @private
%% @doc Initializes the server state and triggers connection.
init([]) ->
	io:format("MQTT Subscriber Started~n"),
	self() ! connect_mqtt,
	timer:send_interval(5000, send_heartbeat),
    % Create a public ETS table for lock-free worker routing
    ets:new(service_subscriber_workers, [set, public, named_table, {read_concurrency, true}]),
    {ok, #state{
		conn_opts = [{host, "mosquitto"}, {port, 1883}, {clientid, <<"erlang_subscriber">>}]
    }}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% @private
%% @doc Handles the 'connect' message to establish connection and subscribe to topics.
%% Also handles the 'send_heartbeat' message to broadcast heartbeats to all workers.
handle_info(connect_mqtt, #state{conn_opts = Opts} = State) ->
    % start and connect client
    {ok, Pid} = emqtt:start_link(Opts),
    % connect may be synchronous in this client
    _ = (catch emqtt:connect(Pid)),
    Topic = <<"sensors/#">>,

    _ = (catch emqtt:subscribe(Pid, {Topic, 0})),
    io:format("Subscribed to ~p~n", [Topic]),
    {noreply, State#state{
        conn_pid = Pid,
        topic = Topic
    }};

handle_info(send_heartbeat, State) ->
    ets:foldl(fun({_Topic, Pid}, Acc) ->
                  Pid ! heartbeat,
                  Acc
              end, ok, service_subscriber_workers),
    {noreply, State};

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
spawn_or_forward(Topic, Payload, State) ->
    case ets:lookup(service_subscriber_workers, Topic) of
        [{Topic, Pid}] ->
            case erlang:is_process_alive(Pid) of
                true ->
                    gen_server:cast(Pid, Payload),
                    {noreply, State};
                false ->
                    ets:delete(service_subscriber_workers, Topic),
                    spawn_and_forward(Topic, Payload),
                    {noreply, State}
            end;
        [] ->
            spawn_and_forward(Topic, Payload),
            {noreply, State}
    end.

spawn_and_forward(Topic, Payload) ->
    spawn(fun() ->
        case service_subscriber_worker_sup:start_worker(Topic) of
            {ok, Pid} ->
                gen_server:cast(Pid, Payload);
            {error, {already_started, Pid}} ->
                ets:insert(service_subscriber_workers, {Topic, Pid}),
                gen_server:cast(Pid, Payload);
            {error, already_present} ->
                {ok, Pid} = supervisor:restart_child(service_subscriber_worker_sup, Topic),
                ets:insert(service_subscriber_workers, {Topic, Pid}),
                gen_server:cast(Pid, Payload)
        end
    end).
