%% @doc The root supervisor for the service_subscriber application.
-module(service_subscriber_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%% @doc Starts the supervisor.
start_link() ->
	io:format("Subscriber Supervisor Started~n"),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @private
%% @doc Initializes the supervision tree.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 3,
        period => 5
    },
    
    % The MQTT subscriber server listens for messages and spawns worker actors
    ChildSpecs = [
        #{id => service_subscriber_mqtt,
          start => {service_subscriber_mqtt, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [service_subscriber_mqtt]}
    ],
    {ok, {SupFlags, ChildSpecs}}.
