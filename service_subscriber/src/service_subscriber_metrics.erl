-module(service_subscriber_metrics).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([inc_requests/0, inc_committed/1, observe_latency/1, observe_e2e_latency/1, observe_db_write_latency/1, set_sensor_status/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

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

%% Records the subscriber-receive-to-now latency for the payload-timestamp summary.
observe_latency(Latency) ->
    prometheus_quantile_summary:observe(subscriber_request_latency_milliseconds, Latency).

%% Records the end-to-end latency from payload timestamp to DB acknowledgement.
observe_e2e_latency(Latency) ->
    prometheus_quantile_summary:observe(subscriber_e2e_latency_milliseconds, Latency).

%% Records the time elapsed between subscriber receive and DB write acknowledgement.
observe_db_write_latency(Latency) ->
    prometheus_quantile_summary:observe(subscriber_db_write_latency_milliseconds, Latency).

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

    prometheus_quantile_summary:declare([
        {name, subscriber_request_latency_milliseconds},
        {help, "Latency of requests in milliseconds (Now - Payload Timestamp)."},
        {labels, []},
        {quantiles, [0.5, 0.95, 0.99, 0.999]}
    ]),

    prometheus_quantile_summary:declare([
        {name, subscriber_e2e_latency_milliseconds},
        {help, "End-to-end latency in milliseconds (DB ack - Payload Timestamp)."},
        {labels, []},
        {quantiles, [0.5, 0.95, 0.99, 0.999]}
    ]),

    prometheus_quantile_summary:declare([
        {name, subscriber_db_write_latency_milliseconds},
        {help, "Latency from subscriber receive to DB write ack, in milliseconds."},
        {labels, []},
        {quantiles, [0.5, 0.95, 0.99, 0.999]}
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
