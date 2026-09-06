%% @doc DB dispatcher — one GenServer per connection (pool of DB_POOL_SIZE workers).
%%
%% Writes are distributed across workers via round-robin. The actual DB
%% operations are delegated to a backend module selected at startup via the
%% DB_BACKEND environment variable (default: timescaledb).
%%
%% Batching lives here, not in the backend: this module owns the row buffer,
%% the BATCH_SIZE / BATCH_TIMEOUT_MS triggers and the flush timer, and hands the
%% backend either a single row (insert/5) or a whole flushed buffer
%% (insert_batch/2). Every backend therefore sees identical batching semantics,
%% which is what makes the swept batch factors comparable across backends.
%% Mirrors the Scala BatchWriterActor -- keep the flush triggers in sync.
%%
%% The backend remains fully responsible for async correlation and latency
%% computation. The dispatcher routes casts, forwards all other incoming
%% messages to the backend via handle_result/2, and records the latency
%% pairs the backend returns.
-module(service_subscriber_db).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, init_counter/0, insert/4, insert_status/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(COUNTER_KEY,   {?MODULE, round_robin_counter}).
-define(POOL_SIZE_KEY, {?MODULE, pool_size}).

%% State of one pool worker.
%% `backend_mod`: The db_backend implementation resolved from DB_BACKEND.
%% `backend_state`: Opaque state owned by that backend.
%% `batch_enabled`: Whether rows are buffered here before being handed to the backend.
%% `batch_size`: Flush threshold in rows.
%% `batch_timeout_ms`: Flush threshold in milliseconds since the first buffered row.
%% `buffer`: Rows awaiting flush, newest first; reversed into arrival order on flush.
%% `buffer_count`: Row count in `buffer`, tracked separately to avoid an O(n) length/1 scan per insert.
%% `timer_ref`: The in-flight flush timer, or undefined when no buffering cycle is open.
-record(state, {
    backend_mod      :: module(),
    backend_state    :: term(),
    batch_enabled    :: boolean(),
    batch_size       :: pos_integer(),
    batch_timeout_ms :: pos_integer(),
    buffer           :: list(),
    buffer_count     :: non_neg_integer(),
    timer_ref        :: reference() | undefined
}).

%% Starts a named DB dispatcher gen_server for the given pool index.
start_link(Index) ->
    Name = worker_name(Index),
    gen_server:start_link({local, Name}, ?MODULE, [Index], []).

%% Initialises the atomic round-robin counter and stores pool size in persistent_term before any inserts.
init_counter() ->
    PoolSize = list_to_integer(os:getenv("DB_POOL_SIZE", "20")),
    Ref = atomics:new(1, [{signed, false}]),
    persistent_term:put(?COUNTER_KEY,   Ref),
    persistent_term:put(?POOL_SIZE_KEY, PoolSize).

%% @doc Async insert of a sensor data row. Non-blocking for the caller.
insert(DeviceName, Value, MsgTimestampUs, ProcessingStartUs) ->
    gen_server:cast(worker_name(next_index()),
                    {insert, DeviceName, Value, MsgTimestampUs, ProcessingStartUs}).

%% @doc Async insert of a sensor status change row. Non-blocking for the caller.
insert_status(DeviceName, Status) ->
    gen_server:cast(worker_name(next_index()), {insert_status, DeviceName, Status}).

%% Reads the batching config (the only place it is read), then resolves the backend module and
%% delegates initialisation to it, passing batch_enabled and batch_size so it can skip resources it
%% will never use and pre-build any whose shape depends on the size. The flush decisions stay here.
%% No timer is armed here: the first buffered row arms it, and arming one against an empty buffer
%% only schedules a wake-up with nothing to flush.
init([Index]) ->
    BatchEnabled = os:getenv("BATCH_ENABLED", "false") =:= "true",
    BatchSize    = list_to_integer(os:getenv("BATCH_SIZE",       "100")),
    BatchTimeMs  = list_to_integer(os:getenv("BATCH_TIMEOUT_MS", "1000")),
    BackendMod   = resolve_backend(),
    {ok, BackendState} = BackendMod:init(Index, #{batch_enabled => BatchEnabled,
                                                  batch_size    => BatchSize}),
    {ok, #state{backend_mod      = BackendMod,
                backend_state    = BackendState,
                batch_enabled    = BatchEnabled,
                batch_size       = BatchSize,
                batch_timeout_ms = BatchTimeMs,
                buffer           = [],
                buffer_count     = 0,
                timer_ref        = undefined}}.

%% No synchronous calls used; satisfy the callback contract.
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% Hands the row straight to the backend when batching is off.
handle_cast({insert, DeviceName, Value, MsgTs, ProcStart},
            #state{batch_enabled = false, backend_mod = Mod, backend_state = BS} = S) ->
    {noreply, apply_insert_result(Mod:insert(BS, DeviceName, Value, MsgTs, ProcStart), S)};

%% Appends the row to the buffer and flushes immediately when the batch size threshold is reached.
%% Uses the tracked counter instead of length/1 so the check stays O(1) regardless of batch size.
handle_cast({insert, DeviceName, Value, MsgTs, ProcStart},
            #state{batch_enabled = true, buffer = Buf,
                   buffer_count = Count, batch_size = BatchSize} = S) ->
    NewCount = Count + 1,
    S1 = S#state{buffer = [{DeviceName, Value, MsgTs, ProcStart} | Buf], buffer_count = NewCount},
    if
        NewCount >= BatchSize ->
            %% Batch full — flush immediately.
            {noreply, flush(S1)};
        NewCount =:= 1 ->
            %% First row in a new batch — ensure timer is running.
            {noreply, schedule_timer(S1)};
        true ->
            {noreply, S1}
    end;

%% Routes an async sensor-status insert to the backend. Never buffered: volume is transition-only.
handle_cast({insert_status, DeviceName, Status},
            #state{backend_mod = Mod, backend_state = BS} = S) ->
    {ok, NewBS} = Mod:insert_status(BS, DeviceName, Status),
    {noreply, S#state{backend_state = NewBS}};

%% Discards unrecognised casts to keep the gen_server running cleanly.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% Flushes any buffered rows when the timeout fires. Deliberately does NOT re-arm: the next row to
%% arrive re-arms via the NewCount =:= 1 branch in handle_cast/2, so at most one timer is ever live
%% per buffering cycle. Re-arming unconditionally here is what let a leaked timer replace itself on
%% every firing, so the live-timer count could only ever grow.
handle_info(flush_batch, #state{buffer = Buf} = State) ->
    %% Cleared before flush/1 so it skips cancelling a timer that has already fired.
    State1 = State#state{timer_ref = undefined},
    case Buf of
        [] -> {noreply, State1};
        _  -> {noreply, flush(State1)}
    end;

%% Forwards every other incoming message to the backend, counts committed rows, and records
%% returned latency pairs.
handle_info(Msg, #state{backend_mod = Mod, backend_state = BS} = S) ->
    case Mod:handle_result(Msg, BS) of
        {match, Latencies, NewBS} ->
            record_committed(Latencies),
            {noreply, S#state{backend_state = NewBS}};
        {no_match, NewBS} ->
            {noreply, S#state{backend_state = NewBS}}
    end.

%% Delegates shutdown cleanup to the backend so it can close the DB connection gracefully.
%% Buffered rows are not drained: a shutdown mid-run is already a failed run, and flushing here
%% would emit a batch whose latencies are dominated by the shutdown itself.
terminate(_Reason, #state{backend_mod = Mod, backend_state = BS}) ->
    Mod:terminate(BS).

%% --- Internal helpers ---

%% Converts a numeric pool index to the registered atom name for that worker.
worker_name(Index) ->
    list_to_atom("service_subscriber_db_" ++ integer_to_list(Index)).

%% Returns the next 1-based pool index using a lock-free atomic round-robin counter.
next_index() ->
    Ref      = persistent_term:get(?COUNTER_KEY),
    PoolSize = persistent_term:get(?POOL_SIZE_KEY),
    N = atomics:add_get(Ref, 1, 1),
    ((N - 1) rem PoolSize) + 1.

%% Reads DB_BACKEND from the environment and returns the corresponding backend module atom.
resolve_backend() ->
    case os:getenv("DB_BACKEND", "timescaledb") of
        "timescaledb" -> db_backend_timescaledb;
        "mysql"       -> db_backend_mysql;
        Unknown       -> error({unknown_db_backend, Unknown})
    end.

%% Sends the whole buffer to the backend as one write and resets the buffering cycle.
%% Cancels the pending flush timer first: clearing timer_ref without cancelling left the timer armed
%% in the VM, so every size-triggered flush orphaned one. cancel_timer/1 returns false if it had
%% already fired, leaving a stale flush_batch in the mailbox -- that costs at most one early flush
%% and cannot accumulate. The Scala BatchWriterActor's timers.cancel has the same benign race.
flush(#state{backend_mod = Mod, backend_state = BS, buffer = Buf, timer_ref = TRef} = S) ->
    _ = case TRef of
        undefined -> ok;
        _         -> erlang:cancel_timer(TRef)
    end,
    S1 = S#state{buffer = [], buffer_count = 0, timer_ref = undefined},
    apply_insert_result(Mod:insert_batch(BS, lists:reverse(Buf)), S1).

%% Arms a one-shot timer to flush the buffer after batch_timeout_ms if no timer is already running.
schedule_timer(#state{timer_ref = undefined, batch_timeout_ms = Ms} = State) ->
    State#state{timer_ref = erlang:send_after(Ms, self(), flush_batch)};
%% Returns state unchanged when a timer is already in flight to avoid double-scheduling.
schedule_timer(State) ->
    State.

%% Stores the backend's new state and, for a backend that wrote synchronously, records the rows it
%% already measured. Async writes report nothing here; their ack lands in handle_info/2 instead.
apply_insert_result({async, NewBS}, S) ->
    S#state{backend_state = NewBS};
apply_insert_result({sync, {E2E, Sub}, NewBS}, S) ->
    record_committed([{E2E, Sub}]),
    S#state{backend_state = NewBS};
apply_insert_result({sync, Latencies, NewBS}, S) when is_list(Latencies) ->
    record_committed(Latencies),
    S#state{backend_state = NewBS}.

%% The single definition of "these rows are committed": one counter bump for the whole ack and one
%% latency pair per row, so the sync and async paths cannot disagree on what a commit means.
%% An empty list is a claimed-but-failed write: nothing committed, nothing recorded. Counting it
%% would inflate committed_s precisely when writes start failing.
record_committed([]) ->
    ok;
record_committed(Latencies) ->
    service_subscriber_metrics:inc_committed(length(Latencies)),
    lists:foreach(fun({E2E, Sub}) -> record_latencies(E2E, Sub) end, Latencies).

%% Converts a microsecond latency pair to native time units and forwards it to the histograms.
%% Native keeps observations integral, which is the difference between one ets:update_counter and
%% a per-observation match-spec rebuild.
record_latencies(E2EUs, SubToDbUs) ->
    service_subscriber_metrics:observe_e2e_latency(
        erlang:convert_time_unit(E2EUs, microsecond, native)),
    service_subscriber_metrics:observe_db_write_latency(
        erlang:convert_time_unit(SubToDbUs, microsecond, native)).
