%% @doc Read-load generator -- one GenServer per reader (pool of READ_POOL_SIZE).
%%
%% Each reader owns one db_read_backend instance and its connection, runs one
%% read at a time, and paces itself. Nothing is routed to a reader: unlike the DB
%% dispatcher there is no round-robin, because readers drive themselves rather
%% than serving traffic from the ingest path.
%%
%% This is deliberately artificial load. Its purpose is to put concurrent query
%% pressure on the database and on the runtime's schedulers while ingestion runs,
%% so that read rate can be swept as a benchmark factor.
%%
%% Pacing lives here, not in the backend: this module is the only reader of
%% READS_PER_SEC and READ_POOL_SIZE, so every backend gets identical cadence
%% semantics -- the same split service_subscriber_db uses to keep the batch
%% factors from being reinterpreted per backend.
%%
%% READS_PER_SEC = 0 means no readers at all: reader_count/0 returns 0 and the
%% supervisor builds no child specs, so nothing here runs and no connection is
%% opened.
-module(service_subscriber_reader).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, reader_count/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% State of one reader.
%% `backend_mod`: The db_read_backend implementation resolved from DB_BACKEND.
%% `backend_state`: Opaque state owned by that backend, including its connection.
%% `period_native`: This reader's share of the aggregate target rate, as a period.
%% `next_due`: Monotonic deadline for the next read; advanced by exactly one period per cycle.
-record(state, {
    backend_mod   :: module(),
    backend_state :: term(),
    period_native :: non_neg_integer(),
    next_due      :: integer()
}).

%% --- API Functions ---

%% Starts a named reader gen_server for the given reader index.
start_link(Index) ->
    gen_server:start_link({local, reader_name(Index)}, ?MODULE, [Index], []).

%% How many readers the supervisor should start: READ_POOL_SIZE when reads are enabled, and 0 when
%% READS_PER_SEC is 0. Returning 0 is what makes "no reads" mean no actors, no connections and no
%% timers, rather than idle readers that still count against the connection budget.
reader_count() ->
    case reads_per_sec() of
        0 -> 0;
        _ -> read_pool_size()
    end.

%% --- gen_server Callbacks ---

%% Reads the read-load config (the only place it is read), resolves the backend and opens its
%% connection, then fires the first read immediately. Division is safe because the supervisor only
%% builds reader children when reader_count/0 is non-zero, which requires READS_PER_SEC > 0.
init([Index]) ->
    BackendMod = resolve_read_backend(),
    {ok, BackendState} = BackendMod:init(Index),
    %% READS_PER_SEC is the aggregate across every reader, so each one targets its own share of it.
    %% Keeping the knob aggregate is what leaves READ_POOL_SIZE a pure concurrency/connection
    %% setting: changing it redistributes the same total read load instead of scaling it.
    %% Mirrors the period computed in Reader.scala -- keep the formula in sync.
    PeriodMs = round(1000 * read_pool_size() / reads_per_sec()),
    schedule_read(0),
    {ok, #state{backend_mod   = BackendMod,
                backend_state = BackendState,
                period_native = erlang:convert_time_unit(PeriodMs, millisecond, native),
                next_due      = erlang:monotonic_time()}}.

%% No synchronous calls used; satisfy the callback contract.
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% No casts used; satisfy the callback contract.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% Runs one read, records it, and arms the next against a fixed deadline.
%%
%% The deadline advances by exactly one period per cycle, and the timer is set to whatever is left
%% until it. Sleeping period-minus-query-time instead would leave every cycle carrying whatever the
%% runtime spends outside the measured read -- timer granularity, dispatch, GC -- which measured as a
%% constant ~20ms per cycle in the Scala arm against ~1ms here. That is a per-arm constant, so the
%% two arms would have run measurably different read loads at the same READS_PER_SEC. Pacing to a
%% deadline absorbs a constant lateness completely: the next deadline is already set, so a late
%% firing simply shortens the next sleep.
%%
%% The deadline is never left in the past (the max/2 below), so a reader that cannot keep up
%% degrades to running flat out rather than accumulating a debt it would later burn off in a burst.
%% Combined with arming only from a completed read, at most one read is ever in flight per reader:
%% a repeating fixed-rate timer would instead keep enqueueing reads a slow database cannot serve,
%% growing the mailbox without bound while the knob silently stopped describing the read rate.
%% When the target is genuinely unreachable the achieved rate simply falls below it -- which is why
%% reports must use subscriber_reads_total, not the configured value.
handle_info(read, #state{backend_mod = Mod, backend_state = BS,
                         period_native = PeriodNative, next_due = NextDue} = State) ->
    Start = erlang:monotonic_time(),
    Outcome = Mod:read(BS),
    ElapsedNative = erlang:monotonic_time() - Start,
    NewBS = case Outcome of
        {ok, B} ->
            service_subscriber_metrics:inc_reads(),
            service_subscriber_metrics:observe_read_latency(ElapsedNative),
            B;
        {error, Reason, B} ->
            %% Left uncounted for the same reason a failed write is never counted as committed:
            %% counting it would hold the read rate up precisely when reads start failing.
            ?LOG_WARNING("db read failed: ~p", [Reason]),
            B
    end,
    Now = erlang:monotonic_time(),
    NewDue = max(NextDue + PeriodNative, Now),
    schedule_read(erlang:convert_time_unit(NewDue - Now, native, millisecond)),
    {noreply, State#state{backend_state = NewBS, next_due = NewDue}};

%% Discards unrecognised messages to keep the gen_server running cleanly.
handle_info(_Msg, State) ->
    {noreply, State}.

%% Delegates shutdown cleanup to the backend so it can close its connection gracefully.
terminate(_Reason, #state{backend_mod = Mod, backend_state = BS}) ->
    Mod:terminate(BS).

%% --- Internal helpers ---

%% Converts a numeric reader index to the registered atom name for that reader.
reader_name(Index) ->
    list_to_atom("service_subscriber_reader_" ++ integer_to_list(Index)).

%% Aggregate target read rate across all readers; 0 disables reads entirely.
reads_per_sec() ->
    list_to_integer(os:getenv("READS_PER_SEC", "0")).

%% Number of readers, and so the number of PostgreSQL connections reads add on top of DB_POOL_SIZE.
%% Deliberately independent of PUBLISHER_COUNT, so the read group's connection count stays constant
%% and cannot drift if the benchmark anchor's publisher count is ever changed.
read_pool_size() ->
    list_to_integer(os:getenv("READ_POOL_SIZE", "4")).

%% Reads DB_BACKEND from the environment and returns the corresponding read backend module atom.
resolve_read_backend() ->
    case os:getenv("DB_BACKEND", "timescaledb") of
        "timescaledb" -> db_read_backend_timescaledb;
        Unknown       -> error({unknown_db_backend, Unknown})
    end.

%% Arms the one-shot timer for the next read. No timer reference is kept: exactly one is ever in
%% flight per reader, so there is nothing to cancel and nothing that can leak.
schedule_read(DelayMs) ->
    erlang:send_after(DelayMs, self(), read).
