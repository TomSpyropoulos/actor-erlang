-module(service_subscriber_mqtt).
-behaviour(gen_server).

%% public API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("kernel/include/logger.hrl").

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

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% --- gen_server Callbacks ---

init([]) ->
	?LOG_INFO("MQTT Subscriber Started"),
	self() ! connect_mqtt,
	timer:send_interval(5000, send_heartbeat),
    %% public + read_concurrency: workers insert their own ETS entry and callers read without locks.
    ets:new(service_subscriber_workers, [set, public, named_table, {read_concurrency, true}]),
    {ok, #state{
		conn_opts = [{host, "mosquitto"}, {port, 1883}, {clientid, <<"erlang_subscriber">>}]
    }}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_info(connect_mqtt, #state{conn_opts = Opts} = State) ->
    {ok, Pid} = emqtt:start_link(Opts),
    %% emqtt:connect/1 may block; catch avoids crashing the gen_server on transient errors.
    _ = (catch emqtt:connect(Pid)),
    Topic = <<"sensors/#">>,

    _ = (catch emqtt:subscribe(Pid, {Topic, 0})),
    ?LOG_INFO("Subscribed to ~p", [Topic]),
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

handle_info({publish, #{topic := Topic, payload := Payload}}, State) when is_binary(Topic) ->
	spawn_or_forward(Topic, Payload, State);

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    %% Linear scan is acceptable: the table is tiny (one entry per unique sensor topic)
    %% and DOWN events are rare (only on worker crash/stop).
    ets:match_delete(service_subscriber_workers, {'_', Pid}),
    {noreply, State}.

terminate(_Reason, #state{conn_pid = MqttPid}) ->
    case is_pid(MqttPid) of
        true  -> emqtt:disconnect(MqttPid);
        false -> ok
    end,
    ok.

%% Internal helpers

spawn_or_forward(Topic, Payload, State) ->
    case ets:lookup(service_subscriber_workers, Topic) of
        [{Topic, Pid}] ->
            %% Worker is known — cast directly. No is_process_alive check needed;
            %% if the worker is dead we'll receive a 'DOWN' message and clean up ETS.
            gen_server:cast(Pid, Payload),
            {noreply, State};
        [] ->
            spawn_and_forward(Topic, Payload),
            {noreply, State}
    end.

%% Spawned so supervisor:start_child/2 (potentially slow) doesn't block the mqtt gen_server.
spawn_and_forward(Topic, Payload) ->
    spawn(fun() ->
        case service_subscriber_worker_sup:start_worker(Topic) of
            {ok, Pid} ->
                %% Monitor the new worker so we can clean up ETS if it crashes.
                erlang:monitor(process, Pid),
                gen_server:cast(Pid, Payload);
            {error, {already_started, Pid}} ->
                erlang:monitor(process, Pid),
                ets:insert(service_subscriber_workers, {Topic, Pid}),
                gen_server:cast(Pid, Payload);
            {error, already_present} ->
                {ok, Pid} = supervisor:restart_child(service_subscriber_worker_sup, Topic),
                erlang:monitor(process, Pid),
                ets:insert(service_subscriber_workers, {Topic, Pid}),
                gen_server:cast(Pid, Payload)
        end
    end).
