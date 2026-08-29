-module(service_subscriber_metrics).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([inc_requests/0, inc_committed/1, observe_latency/1, observe_e2e_latency/1, observe_db_write_latency/1, inc_reads/0, observe_read_latency/1, set_sensor_status/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

%% Latency histogram bounds in milliseconds, shared verbatim with the Scala arm's Metrics.scala.
%% Changing them in one repo silently destroys cross-arm latency comparability, and changing them
%% at all invalidates comparison against any previously collected sweep. Declared as floats so the
%% exposed `le` labels match the Scala client's formatting exactly. prometheus.erl converts these
%% to native units at declare time (the name ends in _milliseconds), which is why observations
%% arrive in native units — see observe_latency/1.
-define(LATENCY_BUCKETS, [
    0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.4,
    0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 7.5, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0, 75.0,
    100.0, 150.0, 200.0, 300.0, 400.0, 500.0, 750.0, 1000.0, 1500.0, 2000.0, 3000.0, 5000.0,
    7500.0, 10000.0, 20000.0, 60000.0
]).

%% --- API ---

%% Starts the metrics gen_server and registers it locally so API functions can reach it.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Increments the total message counter by one for each processed sensor payload.
inc_requests() ->
    prometheus_counter:inc(subscriber_requests_total).

%% Increments the committed-rows counter by N on the DB-ack path (N=1 single insert, batch length otherwise).
inc_committed(N) ->
    prometheus_counter:inc(subscriber_committed_total, N).

%% Records the subscriber-receive-to-now latency. Takes an integer in Erlang's native time unit:
%% prometheus_histogram:observe/2 dispatches integers to a single ets:update_counter, while a float
%% takes a path that rebuilds a 40-atom ETS match spec per observation — measured at +69% subscriber
%% CPU at 20k msg/s. Native also keeps sub-millisecond resolution that integer ms would lose.
observe_latency(LatencyNative) ->
    prometheus_histogram:observe(subscriber_request_latency_milliseconds, LatencyNative).

%% Records the end-to-end latency from payload timestamp to DB acknowledgement, in native units.
observe_e2e_latency(LatencyNative) ->
    prometheus_histogram:observe(subscriber_e2e_latency_milliseconds, LatencyNative).

%% Records the time elapsed between subscriber receive and DB write acknowledgement, in native units.
observe_db_write_latency(LatencyNative) ->
    prometheus_histogram:observe(subscriber_db_write_latency_milliseconds, LatencyNative).

%% Increments the completed-read counter by one. Reads that failed are deliberately not counted,
%% so the read rate does not hold steady while queries are erroring out.
inc_reads() ->
    prometheus_counter:inc(subscriber_reads_total).

%% Records how long one read took, in native units for the same reason as observe_latency/1: an
%% integer observation is a single ets:update_counter, a float rebuilds a 40-atom match spec.
observe_read_latency(LatencyNative) ->
    prometheus_histogram:observe(subscriber_read_latency_milliseconds, LatencyNative).

%% Updates the per-device liveness gauge to 1 (ALIVE) or 0 (MISSING) on every heartbeat.
set_sensor_status(DeviceName, <<"ALIVE">>) ->
    prometheus_gauge:set(subscriber_sensor_up, [DeviceName], 1);
%% Sets the liveness gauge to 0 for any status value other than ALIVE.
set_sensor_status(DeviceName, _Missing) ->
    prometheus_gauge:set(subscriber_sensor_up, [DeviceName], 0).

%% --- gen_server callbacks ---

%% Declares all Prometheus metrics at startup so they exist before any observation arrives.
init([]) ->
    prometheus_counter:declare([
        {name, subscriber_requests_total},
        {help, "Total requests processed by the subscriber."}
    ]),

    prometheus_counter:declare([
        {name, subscriber_committed_total},
        {help, "Total rows committed to the database."}
    ]),

    %% duration_unit is deliberately left inferred from the _milliseconds suffix: prometheus.erl
    %% then converts the bounds above to native units at declare time and converts _sum back on
    %% exposition, so `le` labels and _sum stay in milliseconds and match the Scala arm exactly.
    prometheus_histogram:declare([
        {name, subscriber_request_latency_milliseconds},
        {help, "Latency of requests in milliseconds (Now - Payload Timestamp)."},
        {labels, []},
        {buckets, ?LATENCY_BUCKETS}
    ]),

    prometheus_histogram:declare([
        {name, subscriber_e2e_latency_milliseconds},
        {help, "End-to-end latency in milliseconds (DB ack - Payload Timestamp)."},
        {labels, []},
        {buckets, ?LATENCY_BUCKETS}
    ]),

    prometheus_histogram:declare([
        {name, subscriber_db_write_latency_milliseconds},
        {help, "Latency from subscriber receive to DB write ack, in milliseconds."},
        {labels, []},
        {buckets, ?LATENCY_BUCKETS}
    ]),

    prometheus_counter:declare([
        {name, subscriber_reads_total},
        {help, "Total read queries completed against the database."}
    ]),

    %% Same bucket bounds as the write-path histograms: the read stage is reported through the same
    %% pooled-quantile machinery, so it has to share the bounds to be sliceable the same way.
    prometheus_histogram:declare([
        {name, subscriber_read_latency_milliseconds},
        {help, "Latency of read queries in milliseconds."},
        {labels, []},
        {buckets, ?LATENCY_BUCKETS}
    ]),

    prometheus_gauge:declare([
        {name, subscriber_sensor_up},
        {help, "Sensor liveness: 1 = ALIVE, 0 = MISSING."},
        {labels, [device]}
    ]),

    {ok, #{}}.

%% No synchronous calls used; satisfy the callback contract.
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% No casts used; satisfy the callback contract.
handle_cast(_Request, State) ->
    {noreply, State}.

%% No out-of-band messages expected; satisfy the callback contract.
handle_info(_Info, State) ->
    {noreply, State}.

%% Nothing to clean up on shutdown; Prometheus metrics live in the registry process.
terminate(_Reason, _State) ->
    ok.
