%% @doc Behaviour for pluggable database write backends. Each of the dispatcher's pool workers
%% delegates its DB operations to one instance of the module DB_BACKEND selects.
%%
%% Buffering is NOT part of this contract. The dispatcher (service_subscriber_db) owns the buffer,
%% the BATCH_SIZE / BATCH_TIMEOUT_MS triggers and the flush timer, and calls insert/5 or
%% insert_batch/2 accordingly, so no backend can reinterpret what the swept batch factors mean.
%%
%% To add a backend: implement all six callbacks, then add a clause in
%% service_subscriber_db:resolve_backend/0.
-module(db_backend).

%% Called once per pool worker at startup. Index is the worker's slot number (1..POOL_SIZE), usable
%% to name per-worker resources such as prepared statements.
%%
%% Opts is passed in rather than re-read from the environment, so BATCH_ENABLED and BATCH_SIZE keep a
%% single reader. Use it only to decide which resources to set up, never to make flush decisions.
%% batch_size is here for a backend whose batch statement has fixed arity (db_backend_mysql);
%% db_backend_timescaledb takes array parameters and ignores it.
-callback init(Index :: non_neg_integer(),
               Opts  :: #{batch_enabled := boolean(), batch_size := pos_integer()}) ->
    {ok, BackendState :: term()}.

%% Submit one sensor-data row. Called only when batching is off.
%%
%% MsgTimestampUs is both the value written to the Timestamp column and the start of the e2e latency
%% measurement; the backend converts it to whatever its driver wants, so no DB-shaped type crosses
%% this contract. ProcStartUs is passed through only to compute the subscriber->DB latency.
%%
%% Return {async, NewState} if the write was dispatched asynchronously: handle_result/2 is then
%% called for every subsequent message until the backend claims the ack. Return {sync, ...} if the
%% write completed inline and the backend measured the latency itself.
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

%% Called by the dispatcher's handle_info/2 for every message the worker receives. Return
%% {match, Latencies, NewState} for an async-query result this backend owns, with one
%% {E2EUs, SubToDbUs} pair per acknowledged row; an empty list claims the message but reports nothing
%% committed, which is how a backend signals that its write failed. Anything else must return
%% {no_match, NewState}.
-callback handle_result(Message :: term(), BackendState :: term()) ->
    {match,    [{E2EUs :: integer(), SubToDbUs :: integer()}], NewBackendState :: term()} |
    {no_match, NewBackendState :: term()}.

%% Called when the pool worker is shutting down. Close connections and
%% release any resources held in BackendState.
-callback terminate(BackendState :: term()) -> ok.
