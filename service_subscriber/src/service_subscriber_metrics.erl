-module(service_subscriber_metrics).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([inc_requests/0, observe_latency/1]).

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

%% --- gen_server callbacks ---

init([]) ->
    % 1. Register metrics
    prometheus_counter:declare([
        {name, subscriber_requests_total},
        {help, "Total requests processed by the subscriber."}
    ]),

    %% Declare a Prometheus Quantile Summary for tracking latency distribution.
    %%
    %% Unlike a standard Histogram (which requires predefined static buckets and calculates
    %% percentiles on the Prometheus server), a Quantile Summary calculates accurate percentiles
    %% directly on the client side using a streaming algorithm over a sliding time window.
    %%
    %% Note on time units: Because the metric name ends in `_milliseconds`, the Prometheus client
    %% expects values passed to `observe/2` to be in Erlang's *native* time unit. It will then
    %% automatically convert the native value to milliseconds when exporting the metrics.
    prometheus_quantile_summary:declare([
        {name, subscriber_request_latency_milliseconds},
        {help, "Latency of requests in milliseconds (Now - Payload Timestamp)."},
        {labels, []},
        %% Define the specific quantiles we want to track (P50, P95, P99, P999)
        {quantiles, [0.5, 0.95, 0.99, 0.999]}
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
