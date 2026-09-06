%% @doc Behaviour defining the interface for pluggable database write backends.
%%
%% The dispatcher (service_subscriber_db) holds a pool of N GenServer workers.
%% Each worker delegates all DB operations to a backend module that implements
%% this behaviour. The active backend is selected at runtime via the DB_BACKEND
%% environment variable (see service_subscriber_db:resolve_backend/0).
%%
%% Buffering is NOT part of this contract. The dispatcher owns the buffer, the
%% BATCH_SIZE / BATCH_TIMEOUT_MS triggers and the flush timer, and calls either
%% insert/5 (batching off) or insert_batch/2 (batching on). A backend therefore
%% cannot re-implement batching, and cannot diverge from the other backends on
%% what the swept batch factors mean.
%%
%% To add a new backend: implement all six callbacks below, then add a clause
%% for its name string in service_subscriber_db:resolve_backend/0.
-module(db_backend).

%% Called once per pool worker at startup. Index is the worker's slot number
%% (1..POOL_SIZE) and can be used to name per-worker resources such as
%% prepared statements.
%%
%% Opts carries the dispatcher's batching decision as
%% #{batch_enabled => boolean(), batch_size => pos_integer()}. Both are passed in
%% rather than re-read from the environment so BATCH_ENABLED and BATCH_SIZE keep a
%% single reader; a backend should use them only to decide which resources to set up
%% (e.g. skip preparing a batch statement that will never be executed), never to make
%% its own flush decisions -- the dispatcher owns those. batch_size is here because a
%% backend whose batch statement has fixed arity (a multi-row VALUES list, as in
%% db_backend_mysql) must know the size to pre-build it, while one taking array
%% parameters (db_backend_timescaledb) does not and ignores it.
-callback init(Index :: non_neg_integer(),
               Opts  :: #{batch_enabled := boolean(), batch_size := pos_integer()}) ->
    {ok, BackendState :: term()}.

%% Submit one sensor-data row. Called only when batching is off.
%%
%% MsgTimestampUs is both the value written to the Timestamp column and the
%% start of the e2e latency measurement; the backend converts it to whatever
%% representation its driver wants, so no DB-shaped type crosses this contract.
%% ProcStartUs is passed through only to compute the subscriber->DB latency.
%%
%% Return {async, NewState} if the write was dispatched asynchronously.
%% An ack will arrive later as a message to the dispatcher process;
%% handle_result/2 will be called for every subsequent message until the
%% backend claims it and returns {match, Latencies, NewState}.
%%
%% Return {sync, {E2EUs, SubToDbUs}, NewState} if the write completed
%% inline and the backend has already measured the latency itself.
-callback insert(BackendState    :: term(),
                 DeviceName      :: binary(),
                 Value           :: integer(),
                 MsgTimestampUs  :: integer(),
                 ProcStartUs     :: integer()) ->
    {async, NewBackendState :: term()} |
    {sync, {E2EUs :: integer(), SubToDbUs :: integer()}, NewBackendState :: term()}.

%% Submit a whole flushed buffer as one write. Called only when batching is on.
%%
%% Rows are {DeviceName, Value, MsgTimestampUs, ProcStartUs} tuples in arrival
%% order, with the same field meanings as insert/5. The dispatcher decides when
%% this fires; the backend decides only how to write it (a multi-row statement,
%% a driver-level batch, N single writes).
%%
%% Returns as insert/5, except that {sync, ...} carries one latency pair per row.
-callback insert_batch(BackendState :: term(),
                       Rows :: [{DeviceName     :: binary(),
                                 Value          :: integer(),
                                 MsgTimestampUs :: integer(),
                                 ProcStartUs    :: integer()}]) ->
    {async, NewBackendState :: term()} |
    {sync, [{E2EUs :: integer(), SubToDbUs :: integer()}], NewBackendState :: term()}.

%% Submit one sensor-status row. No latency tracking is required here.
-callback insert_status(BackendState :: term(),
                        DeviceName   :: binary(),
                        Status       :: binary()) ->
    {ok, NewBackendState :: term()}.

%% Called by the dispatcher's handle_info/2 for every message the worker
%% process receives. The backend inspects Message and returns
%% {match, Latencies, NewState} if it recognises it as an async-query result
%% it owns, where Latencies is a list of {E2EUs, SubToDbUs} pairs — one per
%% row that was acknowledged (one for a single insert, N for a batch).
%% An empty list claims the message but reports nothing committed, which is
%% how a backend signals that the write it owned failed.
%% The dispatcher records all latency pairs. Unrecognised messages must
%% return {no_match, NewState}.
-callback handle_result(Message :: term(), BackendState :: term()) ->
    {match,    [{E2EUs :: integer(), SubToDbUs :: integer()}], NewBackendState :: term()} |
    {no_match, NewBackendState :: term()}.

%% Called when the pool worker is shutting down. Close connections and
%% release any resources held in BackendState.
-callback terminate(BackendState :: term()) -> ok.
