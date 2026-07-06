%% OTP application entry point. Starts the root supervision tree.
-module(service_publisher_app).
-behaviour(application).

-include_lib("kernel/include/logger.hrl").

-export([start/2, stop/1]).

%% Starts the root supervision tree when the application boots.
start(_Type, _Args) ->
    ?LOG_INFO("Publisher App Started"),
    service_publisher_sup:start_link().

%% No cleanup needed on application stop.
stop(_State) ->
    ok.
