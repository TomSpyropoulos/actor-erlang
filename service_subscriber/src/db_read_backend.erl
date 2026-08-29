%% @doc Behaviour defining the interface for pluggable database read backends.
%%
%% The reader (service_subscriber_reader) holds a pool of READ_POOL_SIZE
%% GenServers, each owning one backend instance and its connection. The active
%% backend is selected at runtime via the DB_BACKEND environment variable
%% (see service_subscriber_reader:resolve_read_backend/0).
%%
%% Cadence is NOT part of this contract. The reader owns READS_PER_SEC, the
%% per-reader period, the timer that re-arms after each read, and the metric
%% recording. A backend therefore cannot re-implement pacing, and cannot diverge
%% from the other backends on what the swept read rate means -- the same split
%% db_backend uses to keep the batch factors backend-independent.
%%
%% This is deliberately artificial load: it exists to put concurrent query
%% pressure on the database and on the runtime's schedulers while ingestion runs,
%% not to simulate a realistic query mix.
%%
%% To add a new backend: implement all three callbacks below, then add a clause
%% for its name string in service_subscriber_reader:resolve_read_backend/0.
-module(db_read_backend).

%% Called once per reader at startup. Index is the reader's slot number
%% (1..READ_POOL_SIZE) and can be used to name per-reader resources such as
%% prepared statements.
-callback init(Index :: non_neg_integer()) -> {ok, BackendState :: term()}.

%% Execute one read and wait for it to finish.
%%
%% Synchronous by design, unlike the write path's async insert: a reader has
%% nothing else to do while the query runs, and returning only once the result is
%% in hand is what lets the caller time the read and re-arm its own timer without
%% correlating an ack. The result set is discarded -- this is load, not a query
%% whose answer anyone reads.
-callback read(BackendState :: term()) ->
    {ok, NewBackendState :: term()} |
    {error, Reason :: term(), NewBackendState :: term()}.

%% Called when the reader is shutting down. Close connections and release any
%% resources held in BackendState.
-callback terminate(BackendState :: term()) -> ok.
