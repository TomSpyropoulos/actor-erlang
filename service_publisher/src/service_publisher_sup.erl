%% @doc The root supervisor for the service_publisher application.
-module(service_publisher_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%% @doc Starts the supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
	io:format("Publisher Supervisor Started~n"),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @private
%% @doc Initializes the supervision tree.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 3,
        period => 5
    },
    
    % The publisher server handles MQTT connection and periodic tasks
    ChildSpecs = [
        #{id => service_publisher_srv,
          start => {service_publisher_srv, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [service_publisher_srv]}
    ],
    {ok, {SupFlags, ChildSpecs}}.
