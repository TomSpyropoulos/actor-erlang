%% @doc The entry point for the service_publisher Erlang application.
-module(service_publisher_app).
-behaviour(application).

-export([start/2, stop/1]).

%% @private
start(_Type, _Args) ->
	io:format("Publisher App Started~n"),
    service_publisher_sup:start_link().

%% @private
stop(_State) ->
    ok.
