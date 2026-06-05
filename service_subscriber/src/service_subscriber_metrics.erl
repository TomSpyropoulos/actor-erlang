-module(service_subscriber_metrics).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([inc_requests/0, observe_latency/1, observe_e2e_latency/1, observe_db_write_latency/1, set_sensor_status/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

%% --- API ---

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

inc_requests() ->
    prometheus_counter:inc(subscriber_requests_total).

observe_latency(Latency) ->
    prometheus_quantile_summary:observe(subscriber_request_latency_milliseconds, Latency).

observe_e2e_latency(Latency) ->
    prometheus_quantile_summary:observe(subscriber_e2e_latency_milliseconds, Latency).

observe_db_write_latency(Latency) ->
    prometheus_quantile_summary:observe(subscriber_db_write_latency_milliseconds, Latency).

%% @doc Records the current liveness of a sensor as a gauge (1 = ALIVE, 0 = MISSING).
%% Called on every heartbeat so the gauge always reflects the current state.
set_sensor_status(DeviceName, <<"ALIVE">>) ->
    prometheus_gauge:set(subscriber_sensor_up, [DeviceName], 1);
set_sensor_status(DeviceName, _Missing) ->
    prometheus_gauge:set(subscriber_sensor_up, [DeviceName], 0).

%% --- gen_server callbacks ---

init([]) ->
    prometheus_counter:declare([
        {name, subscriber_requests_total},
        {help, "Total requests processed by the subscriber."}
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

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
