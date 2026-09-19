%% @doc MongoDB backend implementing the db_backend behaviour. One mc_worker connection per pool
%% worker. Buffering and the batch triggers belong to the dispatcher (service_subscriber_db); what
%% lives here is how a write is executed and how its ack is correlated back to the rows it covered.
%%
%% mc_worker_api calls block, so writes go through a spawned helper exactly as in db_backend_mysql.
%% Every write waits for the journal, and Data is a time-series collection that stores milliseconds;
%% read findings V through Y in audit.md before comparing this backend with the others.
%%
%% Reads the five DB_* connection variables; their defaults live in docker-compose.mongodb.yaml.
-module(db_backend_mongodb).
-behaviour(db_backend).

-include_lib("kernel/include/logger.hrl").

-export([init/2, insert/5, insert_batch/2, insert_status/3, handle_result/2, terminate/1]).
-export([connect/0]).

%% State of one pool worker's connection.
%% `conn`: The mc_worker process owned by this worker alone.
%% `pending`: Our own ref -> {MsgTimestampUs, ProcStartUs}, or {batch, Rows} for batch acks.
-record(mo_state, {
    conn    :: pid(),
    pending :: #{reference() => {integer(), integer()} | {batch, list()}}
}).

%% Journaled, so an ack means the same fsync the SQL backends' commits mean; see finding W.
%% Mirrors MongoDBBackend.WriteConcern.
-define(WRITE_CONCERN, {<<"w">>, 1, <<"j">>, true}).

%% --- API Functions ---

%% Opens and authenticates one connection. Exported so db_read_backend_mongodb cannot connect any
%% other way. Crashes on failure, so a database that is not up fails the worker at init.
connect() ->
    {ok, Conn} = mc_worker_api:connect([
        {host,     os:getenv("DB_HOST", "mongodb")},
        {port,     list_to_integer(os:getenv("DB_PORT", "27017"))},
        {database, list_to_binary(os:getenv("DB_NAME", "epu"))},
        {login,    list_to_binary(os:getenv("DB_USER", "root"))},
        {password, list_to_binary(os:getenv("DB_PASSWORD", "mongo"))}
    ]),
    Conn.

%% --- db_backend Callbacks ---

%% Opens this worker's connection. Opts is ignored: an insert command takes any number of documents,
%% so nothing depends on the batch size.
init(Index, _Opts) ->
    Conn = connect(),
    ?LOG_INFO("MongoDB backend worker ~p connected", [Index]),
    {ok, #mo_state{conn = Conn, pending = #{}}}.

%% Hands one document's insert to a spawned helper and stores the ref for later latency computation.
insert(#mo_state{conn = Conn, pending = P} = State, DeviceName, Value, MsgTs, ProcStart) ->
    Doc = data_doc(DeviceName, Value, MsgTs),
    Ref = dispatch(fun() -> mc_worker_api:insert(Conn, <<"Data">>, [Doc], ?WRITE_CONCERN) end),
    {async, State#mo_state{pending = P#{Ref => {MsgTs, ProcStart}}}}.

%% Hands a flushed buffer to a spawned helper as one ordered insert command, so a flush of any length
%% waits for one journal commit. Mirrors MongoBatchTarget.scala.
insert_batch(#mo_state{conn = Conn, pending = P} = State, Rows) ->
    Docs = [data_doc(Name, Value, MsgTs) || {Name, Value, MsgTs, _ProcStart} <- Rows],
    Ref = dispatch(fun() -> mc_worker_api:insert(Conn, <<"Data">>, Docs, ?WRITE_CONCERN) end),
    {async, State#mo_state{pending = P#{Ref => {batch, Rows}}}}.

%% Spawns a status write and drops its ack, matching the other backends. reportedat is the client's
%% clock, since MongoDB has no insert-time default; see finding V.
insert_status(#mo_state{conn = Conn} = State, DeviceName, Status) ->
    Doc = #{<<"DeviceName">> => DeviceName, <<"Status">> => Status,
            <<"reportedat">> => os:timestamp()},
    _ = dispatch(fun() -> mc_worker_api:insert(Conn, <<"sensor_status">>, [Doc], ?WRITE_CONCERN) end),
    {ok, State}.

%% Claims a helper's ack matched by ref. A failed write yields no latencies, so the dispatcher counts
%% nothing as committed; the ref is taken on that path too, or `pending` would leak.
handle_result({mongodb_ack, Ref, Result}, #mo_state{pending = P} = State) when is_reference(Ref) ->
    case maps:take(Ref, P) of
        {Pending, Rest} ->
            State1 = State#mo_state{pending = Rest},
            case is_error_result(Result) of
                true ->
                    logger:warning("db write failed, rows not committed: ~p", [Result]),
                    {match, [], State1};
                false ->
                    {match, latencies_for(Pending), State1}
            end;
        error ->
            {no_match, State}
    end;

%% Passes through messages not owned by this backend without modifying state.
handle_result(_Msg, State) ->
    {no_match, State}.

%% Closes the connection when the pool worker shuts down.
terminate(#mo_state{conn = Conn}) ->
    mc_worker_api:disconnect(Conn),
    ok.

%% --- Internal helpers ---

%% db_backend_mysql:dispatch/1's helper, with one difference: mc_worker_api raises on a failed
%% command instead of returning an error, so the raise is caught and acked as a failure rather than
%% killing this worker through the link. A status ack carries a ref nobody registered and is dropped.
dispatch(Fun) ->
    Ref    = make_ref(),
    Parent = self(),
    _ = spawn_link(fun() ->
        Result = try Fun() catch Class:Reason -> {error, {Class, Reason}} end,
        Parent ! {mongodb_ack, Ref, Result}
    end),
    Ref.

%% One reading as a document. The erlang timestamp tuple is what bson-erlang encodes as a Date, and
%% it floors to milliseconds the same way Clock.toDateMillis does.
data_doc(DeviceName, Value, MsgTs) ->
    #{<<"Timestamp">>  => {MsgTs div 1000000000000, (MsgTs div 1000000) rem 1000000, MsgTs rem 1000000},
      <<"DeviceName">> => DeviceName,
      <<"Value">>      => Value}.

%% An insert acks {{true, Reply}, Docs}. ok:1 alone is not success: a rejected document or an
%% unsatisfied write concern comes back as ok:1 with writeErrors or writeConcernError set.
is_error_result({{true, Reply}, _Docs}) when is_map(Reply) ->
    maps:is_key(<<"writeErrors">>, Reply) orelse maps:is_key(<<"writeConcernError">>, Reply);
is_error_result(_) ->
    true.

%% Builds one {E2EUs, SubToDbUs} pair per acknowledged row. The ack clock is read once per ack so
%% every row in a batch shares one flush timestamp.
latencies_for({batch, Rows}) ->
    FlushTime = os:system_time(microsecond),
    [{max(0, FlushTime - MsgTs), max(0, FlushTime - ProcStart)}
     || {_, _, MsgTs, ProcStart} <- Rows];
latencies_for({MsgTs, ProcStart}) ->
    FlushTime = os:system_time(microsecond),
    [{max(0, FlushTime - MsgTs), max(0, FlushTime - ProcStart)}].
