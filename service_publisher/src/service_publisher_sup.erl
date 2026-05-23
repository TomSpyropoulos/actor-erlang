%% @doc Root supervisor for the publisher. Manages a single permanent worker
%%      (service_publisher_srv) that handles MQTT publishing.
-module(service_publisher_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    io:format("Publisher Supervisor Started~n"),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy  => one_for_one,
        intensity => 3,
        period    => 5
    },
    ChildSpecs = [
        #{id      => service_publisher_srv,
          start   => {service_publisher_srv, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type    => worker,
          modules => [service_publisher_srv]}
    ],
    {ok, {SupFlags, ChildSpecs}}.
