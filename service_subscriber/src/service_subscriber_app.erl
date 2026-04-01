%% @doc The entry point for the service_subscriber Erlang application.
-module(service_subscriber_app).
-behaviour(application).

-export([start/2, stop/1]).

%% @private
start(_Type, _Args) ->
	io:format("Subscriber App Started~n"),
    % Start Prometheus metrics server on default port 8081
    prometheus_httpd:start(),
    service_subscriber_sup:start_link().

%% @private
stop(_State) ->
    ok.
