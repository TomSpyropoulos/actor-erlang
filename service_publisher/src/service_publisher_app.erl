%% @doc OTP application entry point. Starts the root supervision tree.
-module(service_publisher_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    io:format("Publisher App Started~n"),
    service_publisher_sup:start_link().

stop(_State) ->
    ok.
