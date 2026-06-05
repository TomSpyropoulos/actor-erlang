%% @doc OTP application entry point. Starts the Prometheus HTTP server and
%%      the root supervision tree.
-module(service_subscriber_app).
-behaviour(application).

-include_lib("kernel/include/logger.hrl").

-export([start/2, stop/1]).

%% Starts the Prometheus HTTP server and then brings up the root supervision tree.
start(_Type, _Args) ->
    ?LOG_INFO("Subscriber App Started"),
    prometheus_httpd:start(),
    service_subscriber_sup:start_link().

%% Nothing to tear down on stop; OTP shuts down the supervision tree automatically.
stop(_State) ->
    ok.
