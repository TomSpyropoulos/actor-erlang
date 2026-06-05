%% @doc DB dispatcher — one GenServer per connection (pool of DB_POOL_SIZE workers).
%%
%% Writes are distributed across workers via round-robin. The actual DB
%% operations are delegated to a backend module selected at startup via the
%% DB_BACKEND environment variable (default: timescaledb).
%%
%% The backend is fully responsible for async correlation and latency
%% computation. The dispatcher simply routes casts, forwards all incoming
%% messages to the backend via handle_result/2, and records the latency
%% pairs the backend returns.
-module(service_subscriber_db).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, init_counter/0, insert/5, insert_status/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(COUNTER_KEY,   {?MODULE, round_robin_counter}).
-define(POOL_SIZE_KEY, {?MODULE, pool_size}).

-record(state, {
    backend_mod   :: module(),
    backend_state :: term()
}).

start_link(Index) ->
    Name = worker_name(Index),
    gen_server:start_link({local, Name}, ?MODULE, [Index], []).

%% Initialise the atomic round-robin counter and store pool size.
%% Must be called before any insert.
init_counter() ->
    PoolSize = list_to_integer(os:getenv("DB_POOL_SIZE", "20")),
    Ref = atomics:new(1, [{signed, false}]),
    persistent_term:put(?COUNTER_KEY,   Ref),
    persistent_term:put(?POOL_SIZE_KEY, PoolSize).

%% @doc Async insert of a sensor data row. Non-blocking for the caller.
insert(DeviceName, Value, ErlTimestamp, MsgTimestampUs, ProcessingStartUs) ->
    gen_server:cast(worker_name(next_index()),
                    {insert, DeviceName, Value, ErlTimestamp, MsgTimestampUs, ProcessingStartUs}).

%% @doc Async insert of a sensor status change row. Non-blocking for the caller.
insert_status(DeviceName, Status) ->
    gen_server:cast(worker_name(next_index()), {insert_status, DeviceName, Status}).

init([Index]) ->
    BackendMod = resolve_backend(),
    {ok, BackendState} = BackendMod:init(Index),
    {ok, #state{backend_mod = BackendMod, backend_state = BackendState}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({insert, DeviceName, Value, ErlTs, MsgTs, ProcStart},
            #state{backend_mod = Mod, backend_state = BS} = S) ->
    case Mod:insert(BS, DeviceName, Value, ErlTs, MsgTs, ProcStart) of
        {async,    NewBS}           -> {noreply, S#state{backend_state = NewBS}};
        {buffered, NewBS}           -> {noreply, S#state{backend_state = NewBS}};
        {sync, {E2E, Sub}, NewBS}   ->
            record_latencies(E2E, Sub),
            {noreply, S#state{backend_state = NewBS}}
    end;

handle_cast({insert_status, DeviceName, Status},
            #state{backend_mod = Mod, backend_state = BS} = S) ->
    {ok, NewBS} = Mod:insert_status(BS, DeviceName, Status),
    {noreply, S#state{backend_state = NewBS}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Route every incoming message through the backend. On a claimed ack the
%% backend returns a list of {E2E, Sub} latency pairs — one per row.
handle_info(Msg, #state{backend_mod = Mod, backend_state = BS} = S) ->
    case Mod:handle_result(Msg, BS) of
        {match, Latencies, NewBS} ->
            lists:foreach(fun({E2E, Sub}) -> record_latencies(E2E, Sub) end, Latencies),
            {noreply, S#state{backend_state = NewBS}};
        {no_match, NewBS} ->
            {noreply, S#state{backend_state = NewBS}}
    end.

terminate(_Reason, #state{backend_mod = Mod, backend_state = BS}) ->
    Mod:terminate(BS).

%% --- Internal helpers ---

worker_name(Index) ->
    list_to_atom("service_subscriber_db_" ++ integer_to_list(Index)).

next_index() ->
    Ref      = persistent_term:get(?COUNTER_KEY),
    PoolSize = persistent_term:get(?POOL_SIZE_KEY),
    N = atomics:add_get(Ref, 1, 1),
    ((N - 1) rem PoolSize) + 1.

resolve_backend() ->
    case os:getenv("DB_BACKEND", "timescaledb") of
        "timescaledb" -> db_backend_timescaledb;
        Unknown       -> error({unknown_db_backend, Unknown})
    end.

%% Converts microsecond durations to native time units before recording;
%% the Prometheus summary expects native units for accurate quantile tracking.
record_latencies(E2EUs, SubToDbUs) ->
    service_subscriber_metrics:observe_e2e_latency(
        erlang:convert_time_unit(E2EUs, microsecond, native)),
    service_subscriber_metrics:observe_db_write_latency(
        erlang:convert_time_unit(SubToDbUs, microsecond, native)).
