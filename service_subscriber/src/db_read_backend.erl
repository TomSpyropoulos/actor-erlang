%% @doc Behaviour for pluggable database read backends. Each of the reader's GenServers owns one
%% instance of the module DB_BACKEND selects, plus its connection.
%%
%% Cadence is NOT part of this contract. service_subscriber_reader owns READS_PER_SEC, the per-reader
%% period, the re-arming timer and the metrics, so no backend can reinterpret the swept read rate.
%% Same split db_backend uses for the batch factors.
%%
%% To add a backend: implement all three callbacks, then add a clause in
%% service_subscriber_reader:resolve_read_backend/0.
-module(db_read_backend).

%% Called once per reader at startup. Index is the reader's slot number
%% (1..READ_POOL_SIZE) and can be used to name per-reader resources such as
%% prepared statements.
-callback init(Index :: non_neg_integer()) -> {ok, BackendState :: term()}.

%% Execute one read and wait for it to finish.
%%
%% Synchronous by design, unlike the write path: returning only once the result is in hand is what
%% lets the caller time the read and re-arm its timer without correlating an ack. The result set is
%% discarded -- this is load, not a query whose answer anyone reads.
-callback read(BackendState :: term()) ->
    {ok, NewBackendState :: term()} |
    {error, Reason :: term(), NewBackendState :: term()}.

%% Called when the reader is shutting down. Close connections and release any
%% resources held in BackendState.
-callback terminate(BackendState :: term()) -> ok.
