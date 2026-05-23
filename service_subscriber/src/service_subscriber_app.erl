%% @doc OTP application entry point. Starts the Prometheus HTTP server and
%%      the root supervision tree.
-module(service_subscriber_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    io:format("Subscriber App Started~n"),
    prometheus_httpd:start(),
    service_subscriber_sup:start_link().

stop(_State) ->
    ok.
