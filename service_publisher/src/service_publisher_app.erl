%% @doc OTP application entry point. Starts the root supervision tree.
-module(service_publisher_app).
-behaviour(application).

-include_lib("kernel/include/logger.hrl").

-export([start/2, stop/1]).

start(_Type, _Args) ->
    ?LOG_INFO("Publisher App Started"),
    service_publisher_sup:start_link().

stop(_State) ->
    ok.
