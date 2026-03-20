% @format
-module(service_subscriber_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
	io:format("App Started~n"),
    service_subscriber_sup:start_link().

stop(_State) ->
    ok.
