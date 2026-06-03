%% @doc Behaviour defining the interface for pluggable database write backends.
%%
%% The dispatcher (service_subscriber_db) holds a pool of 20 GenServer workers.
%% Each worker delegates all DB operations to a backend module that implements
%% this behaviour. The active backend is selected at runtime via the DB_BACKEND
%% environment variable (see service_subscriber_db:resolve_backend/0).
%%
%% To add a new backend: implement all five callbacks below, then add a clause
%% for its name string in service_subscriber_db:resolve_backend/0.
-module(db_backend).

%% Called once per pool worker at startup. Index is the worker's slot number
%% (1..POOL_SIZE) and can be used to name per-worker resources such as
%% prepared statements.
-callback init(Index :: non_neg_integer()) ->
    {ok, BackendState :: term()}.

%% Submit one sensor-data row. MsgTimestampUs and ProcStartUs are passed
%% through so the dispatcher can compute e2e and subscriber->DB latencies on
%% acknowledgement; they are not written to the database.
%%
%% Return {async, Ref, NewState} if the write was dispatched asynchronously.
%% Ref is an opaque correlation token; the dispatcher calls handle_result/2
%% for every subsequent message until the backend claims that Ref.
%%
%% Return {sync, {E2EUs, SubToDbUs}, NewState} if the write completed inline
%% and the backend has already measured the latencies itself.
-callback insert(BackendState    :: term(),
                 DeviceName      :: binary(),
                 Value           :: integer(),
                 Timestamp       :: term(),
                 MsgTimestampUs  :: integer(),
                 ProcStartUs     :: integer()) ->
    {async, Ref :: reference(), NewBackendState :: term()} |
    {sync,  {E2EUs :: integer(), SubToDbUs :: integer()}, NewBackendState :: term()}.

%% Submit one sensor-status row. No latency tracking is required here.
-callback insert_status(BackendState :: term(),
                        DeviceName   :: binary(),
                        Status       :: binary()) ->
    {ok, NewBackendState :: term()}.

%% Called by the dispatcher's handle_info/2 for every message the worker
%% process receives. The backend inspects Message and returns
%% {match, Ref, NewState} if it recognises it as an async-query result it
%% owns; the dispatcher then looks up Ref in its pending map to compute
%% latencies. Unrecognised messages must return {no_match, NewState}.
-callback handle_result(Message :: term(), BackendState :: term()) ->
    {match,    Ref :: reference(), NewBackendState :: term()} |
    {no_match, NewBackendState :: term()}.

%% Called when the pool worker is shutting down. Close connections and
%% release any resources held in BackendState.
-callback terminate(BackendState :: term()) -> ok.
