%% @doc DB dispatcher — one GenServer per connection (pool of 20).
%%
%% Writes are distributed across workers via round-robin. The actual DB
%% operations are delegated to a backend module selected at startup via the
%% DB_BACKEND environment variable (default: timescaledb).
-module(service_subscriber_db).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, init_counter/0, insert/5, insert_status/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(POOL_SIZE, 20).
-define(COUNTER_KEY, {?MODULE, round_robin_counter}).

%% pending maps async-write Refs to {MsgTimestampUs, ProcStartUs} so latencies
%% can be computed when the backend ack arrives in handle_info/2.
-record(state, {
    backend_mod   :: module(),
    backend_state :: term(),
    pending       :: #{reference() => {integer(), integer()}}
}).

start_link(Index) ->
    Name = worker_name(Index),
    gen_server:start_link({local, Name}, ?MODULE, [Index], []).

%% Initialise the atomic round-robin counter. Must be called before any insert.
init_counter() ->
    Ref = atomics:new(1, [{signed, false}]),
    persistent_term:put(?COUNTER_KEY, Ref).

%% Atom name for pool slot Index, e.g. service_subscriber_db_3.
worker_name(Index) ->
    list_to_atom("service_subscriber_db_" ++ integer_to_list(Index)).

next_index() ->
    Ref = persistent_term:get(?COUNTER_KEY),
    N = atomics:add_get(Ref, 1, 1),
    ((N - 1) rem ?POOL_SIZE) + 1.

%% @doc Async insert of a sensor data row. Non-blocking for the caller.
insert(DeviceName, Value, ErlTimestamp, MsgTimestampUs, ProcessingStartUs) ->
    gen_server:cast(worker_name(next_index()),
                    {insert, DeviceName, Value, ErlTimestamp, MsgTimestampUs, ProcessingStartUs}).

%% @doc Async insert of a sensor status change row. Non-blocking for the caller.
insert_status(DeviceName, Status) ->
    gen_server:cast(worker_name(next_index()), {insert_status, DeviceName, Status}).

%% Maps DB_BACKEND to a backend module atom. Crashes on unknown values so
%% misconfiguration is caught at worker startup rather than silently ignored.
resolve_backend() ->
    case os:getenv("DB_BACKEND", "timescaledb") of
        "timescaledb" -> db_backend_timescaledb;
        Unknown       -> error({unknown_db_backend, Unknown})
    end.

init([Index]) ->
    BackendMod = resolve_backend(),
    {ok, BackendState} = BackendMod:init(Index),
    {ok, #state{backend_mod=BackendMod, backend_state=BackendState, pending=#{}}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({insert, DeviceName, Value, ErlTs, MsgTs, ProcStart},
            #state{backend_mod=Mod, backend_state=BS, pending=P} = S) ->
    case Mod:insert(BS, DeviceName, Value, ErlTs, MsgTs, ProcStart) of
        {async, Ref, NewBS} ->
            {noreply, S#state{backend_state=NewBS,
                              pending=P#{Ref => {MsgTs, ProcStart}}}};
        {sync, {E2E, Sub}, NewBS} ->
            record_latencies(E2E, Sub),
            {noreply, S#state{backend_state=NewBS}}
    end;

handle_cast({insert_status, DeviceName, Status},
            #state{backend_mod=Mod, backend_state=BS} = S) ->
    {ok, NewBS} = Mod:insert_status(BS, DeviceName, Status),
    {noreply, S#state{backend_state=NewBS}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Routes every incoming message through the backend. On a claimed async ack,
%% looks up the stored timestamps and records e2e + subscriber->DB latencies.
handle_info(Msg, #state{backend_mod=Mod, backend_state=BS, pending=P} = S) ->
    case Mod:handle_result(Msg, BS) of
        {match, Ref, NewBS} ->
            NewP = case maps:take(Ref, P) of
                {{MsgTs, ProcStart}, Rest} ->
                    AckUs = os:system_time(microsecond),
                    record_latencies(max(0, AckUs - MsgTs),
                                     max(0, AckUs - ProcStart)),
                    Rest;
                error ->
                    P
            end,
            {noreply, S#state{backend_state=NewBS, pending=NewP}};
        {no_match, NewBS} ->
            {noreply, S#state{backend_state=NewBS}}
    end.

terminate(_Reason, #state{backend_mod=Mod, backend_state=BS}) ->
    Mod:terminate(BS).

%% Converts microsecond durations to native time units before recording;
%% the Prometheus summary expects native units for accurate quantile tracking.
record_latencies(E2EUs, SubToDbUs) ->
    service_subscriber_metrics:observe_e2e_latency(
        erlang:convert_time_unit(E2EUs, microsecond, native)),
    service_subscriber_metrics:observe_db_write_latency(
        erlang:convert_time_unit(SubToDbUs, microsecond, native)).
