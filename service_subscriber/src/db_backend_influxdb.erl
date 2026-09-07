%% @doc InfluxDB backend implementing the db_backend behaviour. One instance per pool worker, each
%% owning a named httpc profile capped at one session so DB_POOL_SIZE still bounds write
%% concurrency. Buffering belongs to the dispatcher (service_subscriber_db); what lives here is how
%% a write is executed and how its ack is correlated back to the rows it covered.
%%
%% Writes are HTTP, which changes readiness, what DB_POOL_SIZE counts and whether a repeated write
%% appends; read findings I through O in audit.md before comparing this backend with the other two.
%%
%% Reads the five DB_* variables plus DB_ORG and DB_TOKEN; defaults live in
%% docker-compose.influxdb.yaml.
-module(db_backend_influxdb).
-behaviour(db_backend).

-include_lib("kernel/include/logger.hrl").

-export([init/2, insert/5, insert_batch/2, insert_status/3, handle_result/2, terminate/1]).

%% `profile`: This worker's own httpc profile, so its socket is shared with no other worker.
%% `write_url`: The /api/v2/write URL, built once. precision=us is load-bearing; see finding L.
%% `headers`: Authorization, built once from DB_TOKEN.
%% `pending`: httpc request id -> {MsgTimestampUs, ProcStartUs}, or {batch, Rows} for batch acks.
-record(influx_state, {
    profile   :: atom(),
    write_url :: string(),
    headers   :: [{string(), string()}],
    pending   :: #{term() => {integer(), integer()} | {batch, list()}}
}).

-define(CONTENT_TYPE,       "text/plain; charset=utf-8").
-define(HTTP_TIMEOUT_MS,    10000).
-define(CONNECT_TIMEOUT_MS, 5000).
%% Queue depth of the one keep-alive session, deep enough that a worker's writes never
%% overflow into a throwaway connection. See start_profile/1.
-define(SESSION_QUEUE,      1000).

%% ssl is supplied even though every URL here is plain http: httpc evaluates its default ssl
%% options unless the caller passes some, and that reads the OS CA bundle, which the slim runtime
%% image does not carry.
-define(HTTP_OPTS, [{timeout,         ?HTTP_TIMEOUT_MS},
                    {connect_timeout, ?CONNECT_TIMEOUT_MS},
                    {ssl,             []}]).
%% Attempts x retry must comfortably exceed the container's start_period, or a slow first boot
%% would fail a worker that would have connected a second later.
-define(READY_ATTEMPTS,     60).
-define(READY_RETRY_MS,     1000).

%% --- db_backend Callbacks ---

%% Builds this worker's profile and URLs, then blocks until the bucket answers. Opts is ignored:
%% line protocol has no fixed arity and there is no statement to prepare, so unlike the two SQL
%% backends nothing here depends on batch_size or batch_enabled.
init(Index, _Opts) ->
    Host   = os:getenv("DB_HOST",  "influxdb"),
    Port   = os:getenv("DB_PORT",  "8086"),
    Bucket = os:getenv("DB_NAME",  "epu"),
    Org    = os:getenv("DB_ORG",   "epu"),
    Token  = os:getenv("DB_TOKEN", "epu-benchmark-token"),
    Base    = "http://" ++ Host ++ ":" ++ Port,
    Headers = [{"Authorization", "Token " ++ Token}],
    Profile = start_profile("influx_w" ++ integer_to_list(Index)),
    ok = await_ready(Base ++ "/api/v2/buckets?name=" ++ Bucket, Headers, Profile, ?READY_ATTEMPTS),
    ?LOG_INFO("InfluxDB backend worker ~p ready", [Index]),
    {ok, #influx_state{
        profile   = Profile,
        write_url = Base ++ "/api/v2/write?org=" ++ Org ++ "&bucket=" ++ Bucket ++ "&precision=us",
        headers   = Headers,
        pending   = #{}
    }}.

%% Posts one row and stores its request id for later latency computation.
insert(#influx_state{pending = P} = State, DeviceName, Value, MsgTs, ProcStart) ->
    Id = post(State, line(DeviceName, Value, MsgTs)),
    {async, State#influx_state{pending = P#{Id => {MsgTs, ProcStart}}}}.

%% Posts a flushed buffer as one newline-joined body and registers the batch id. Line protocol has
%% no fixed arity, so unlike db_backend_mysql a short flush needs no fallback of its own.
%% Mirrors InfluxBatchTarget.scala.
insert_batch(#influx_state{pending = P} = State, Rows) ->
    Body = lists:join($\n, [line(Name, Value, MsgTs) || {Name, Value, MsgTs, _} <- Rows]),
    Id = post(State, Body),
    {async, State#influx_state{pending = P#{Id => {batch, Rows}}}}.

%% Posts a status transition and drops its ack, matching the other two backends. The id is never
%% registered, so handle_result/2 sees it unclaimed and passes it through.
insert_status(State, DeviceName, Status) ->
    _ = post(State, status_line(DeviceName, Status)),
    {ok, State}.

%% Claims an httpc response matched by request id and computes latencies for each row it covered.
%% A failed write yields no latencies, so the dispatcher counts nothing as committed: acking an
%% error as a commit would overstate committed_s under exactly the saturation it exists to detect.
handle_result({http, {Id, Result}}, #influx_state{pending = P} = State) ->
    case maps:take(Id, P) of
        {Pending, Rest} ->
            %% Take the id on the error path too -- leaving it behind leaks `pending` for the life
            %% of the worker, the same ratcheting failure as the finding-2 timer leak.
            State1 = State#influx_state{pending = Rest},
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

%% Stops this worker's httpc profile, closing its socket.
terminate(#influx_state{profile = Profile}) ->
    _ = inets:stop(httpc, Profile),
    ok.

%% --- Internal helpers ---

%% Starts this worker's own profile, replacing any left behind by a previous incarnation: a profile
%% is a child of the inets supervisor and outlives the worker, so a restart would otherwise hit
%% {error, {already_started, _}} and crash-loop into the supervisor's restart intensity.
%%
%% One session per profile is what makes DB_POOL_SIZE bound write concurrency. max_keep_alive_length
%% is that session's queue depth, not its wire concurrency, and it must be deep enough to absorb a
%% worker's in-flight writes or httpc opens a throwaway connection for the overflow; finding J in
%% audit.md has the measurements. The runtime atom is bounded by DB_POOL_SIZE and built once per
%% worker, as in service_subscriber_db:worker_name/1.
start_profile(Name) ->
    Profile = list_to_atom(Name),
    _ = inets:stop(httpc, Profile),
    {ok, _} = inets:start(httpc, [{profile, Profile}]),
    ok = httpc:set_options([{max_sessions,          1},
                            {max_keep_alive_length, ?SESSION_QUEUE},
                            {socket_opts,           [{nodelay, true}]}], Profile),
    Profile.

%% Blocks until the bucket answers, so a database that is not up crashes this worker at init the way
%% epgsql and mysql-otp do. HTTP connects lazily, so without this the subscriber would start, ingest
%% happily and commit nothing -- a rep bench.sh would score as valid. See finding I.
await_ready(_Url, _Headers, _Profile, 0) ->
    error(influxdb_not_ready);
await_ready(Url, Headers, Profile, Attempts) ->
    case httpc:request(get, {Url, Headers}, ?HTTP_OPTS, [], Profile) of
        {ok, {{_, 200, _}, _, _}} ->
            ok;
        _ ->
            timer:sleep(?READY_RETRY_MS),
            await_ready(Url, Headers, Profile, Attempts - 1)
    end.

%% Fires one async POST and returns its request id, which is what satisfies {async, State} with no
%% helper process: httpc delivers {http, {Id, Result}} straight to this worker's mailbox.
%%
%% The timeouts are not optional. httpc defaults to infinity, and a response that never arrived
%% would hold this worker's only session forever while its pending map ratcheted, leaving the worker
%% silently unable to commit. mysql-otp gets the same protection from its spawn_link.
post(#influx_state{profile = Profile, write_url = Url, headers = Headers}, Body) ->
    {ok, Id} = httpc:request(post, {Url, Headers, ?CONTENT_TYPE, Body}, ?HTTP_OPTS,
                             [{sync, false}, {body_format, binary}], Profile),
    Id.

%% One reading as a line-protocol point, as an iolist so no copy is made to flatten it. The trailing
%% i keeps Value an integer: field type is fixed by the first write into a shard, so dropping it
%% would silently store floats. Nothing is escaped -- see finding N for why that is safe here.
line(DeviceName, Value, MsgTs) ->
    [<<"Data,DeviceName=">>, DeviceName, <<" Value=">>, integer_to_binary(Value),
     <<"i ">>, integer_to_binary(MsgTs)].

%% One status transition as a point, with no timestamp so the server assigns one -- the equivalent
%% of the DEFAULT NOW() both SQL schemas give reportedat. Status is a string field; the two SQL
%% schemas' CHECK constraint has no InfluxDB equivalent.
status_line(DeviceName, Status) ->
    [<<"sensor_status,DeviceName=">>, DeviceName, <<" Status=\"">>, Status, <<"\"">>].

%% InfluxDB acks a write with 204 and an empty body; any other status, or a transport failure,
%% means the rows did not land.
is_error_result({{_Vsn, 204, _}, _Headers, _Body}) ->
    false;
is_error_result(_) ->
    true.

%% Builds one {E2EUs, SubToDbUs} pair per acknowledged row, reading the ack clock once so every row
%% in a batch shares one flush timestamp. Same shape as its counterpart in the other two backends.
latencies_for({batch, Rows}) ->
    FlushTime = os:system_time(microsecond),
    [{max(0, FlushTime - MsgTs), max(0, FlushTime - ProcStart)}
     || {_, _, MsgTs, ProcStart} <- Rows];
latencies_for({MsgTs, ProcStart}) ->
    FlushTime = os:system_time(microsecond),
    [{max(0, FlushTime - MsgTs), max(0, FlushTime - ProcStart)}].
