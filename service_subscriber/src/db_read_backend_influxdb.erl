%% @doc InfluxDB backend implementing the db_read_backend behaviour. One instance per reader, each
%% owning a named httpc profile capped at one session so READ_POOL_SIZE still bounds read
%% concurrency.
%%
%% The read is issued synchronously, which is what read/1's contract wants, so the async dispatch
%% db_backend_influxdb needs on the write path is deliberately absent here.
%%
%% Reads the five DB_* variables plus DB_ORG and DB_TOKEN; defaults live in
%% docker-compose.influxdb.yaml.
-module(db_read_backend_influxdb).
-behaviour(db_read_backend).

-include_lib("kernel/include/logger.hrl").

-export([init/1, read/1, terminate/1]).

%% State of one reader's HTTP client.
%% `profile`: The httpc profile owned by this reader alone.
%% `url`: The /api/v2/query URL, built once.
%% `headers`: Authorization, built once from DB_TOKEN.
%% `query`: The Flux script with the bucket already interpolated.
-record(infr_state, {
    profile :: atom(),
    url     :: string(),
    headers :: [{string(), string()}],
    query   :: string()
}).

-define(CONTENT_TYPE,       "application/vnd.flux").
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

%% The InfluxDB spelling of the read group's query; see the read-group section of audit.md for why
%% the window is bounded. group() and reduce are load-bearing for equivalence with the SQL backends'
%% avg/count, and finding L records why.
%%
%% Byte-identical to InfluxReadTarget.scala, or the group compares query plans instead of runtimes.
%% The rule is per backend: this pair must match each other, not either SQL pair.
-define(READ_FLUX,
        "from(bucket: \"~s\")\n"
        "  |> range(start: -5s)\n"
        "  |> filter(fn: (r) => r._measurement == \"Data\" and r._field == \"Value\")\n"
        "  |> group()\n"
        "  |> reduce(identity: {count: 0, sum: 0.0},\n"
        "            fn: (r, accumulator) => ({count: accumulator.count + 1,\n"
        "                                      sum: accumulator.sum + float(v: r._value)}))\n"
        "  |> map(fn: (r) => ({mean: r.sum / float(v: r.count), count: r.count}))").

%% Builds this reader's profile, URL and query. No readiness probe, unlike db_backend_influxdb:
%% service_subscriber_sup starts the whole DB pool before the first reader, so the database has
%% already answered by the time this runs.
init(Index) ->
    Host   = os:getenv("DB_HOST",  "influxdb"),
    Port   = os:getenv("DB_PORT",  "8086"),
    Bucket = os:getenv("DB_NAME",  "epu"),
    Org    = os:getenv("DB_ORG",   "epu"),
    Token  = os:getenv("DB_TOKEN", "epu-benchmark-token"),
    Profile = start_profile("influx_r" ++ integer_to_list(Index)),
    ?LOG_INFO("InfluxDB read backend reader ~p ready", [Index]),
    {ok, #infr_state{
        profile = Profile,
        url     = "http://" ++ Host ++ ":" ++ Port ++ "/api/v2/query?org=" ++ Org,
        headers = [{"Authorization", "Token " ++ Token}],
        query   = lists:flatten(io_lib:format(?READ_FLUX, [Bucket]))
    }}.

%% Runs the read to completion and discards the response body. A failed read is reported so the
%% reader leaves it uncounted rather than inflating the read rate with queries that never answered.
read(#infr_state{profile = Profile, url = Url, headers = Headers, query = Query} = State) ->
    case httpc:request(post, {Url, Headers, ?CONTENT_TYPE, Query}, ?HTTP_OPTS,
                       [{body_format, binary}], Profile) of
        {ok, {{_, 200, _}, _, _}}    -> {ok, State};
        {ok, {{_, Code, _}, _, Body}} -> {error, {Code, Body}, State};
        {error, Reason}               -> {error, Reason, State}
    end.

%% Stops this reader's httpc profile, closing its socket.
terminate(#infr_state{profile = Profile}) ->
    _ = inets:stop(httpc, Profile),
    ok.

%% --- Internal helpers ---

%% Starts this reader's own profile, replacing any left behind by a previous incarnation, for the
%% same reason db_backend_influxdb:start_profile/1 does. max_sessions = 1 is what keeps
%% READ_POOL_SIZE the reader connection count.
start_profile(Name) ->
    Profile = list_to_atom(Name),
    _ = inets:stop(httpc, Profile),
    {ok, _} = inets:start(httpc, [{profile, Profile}]),
    ok = httpc:set_options([{max_sessions,          1},
                            {max_keep_alive_length, ?SESSION_QUEUE},
                            {socket_opts,           [{nodelay, true}]}], Profile),
    Profile.
