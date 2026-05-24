%% @doc Dynamic supervisor for per-sensor worker actors.
%%      Workers are added at runtime via start_worker/1 when a new MQTT topic
%%      is first seen. Each child is keyed by its topic binary.
-module(service_subscriber_worker_sup).
-behaviour(supervisor).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, start_worker/1, init/1]).

start_link() ->
    ?LOG_INFO("Worker Supervisor Started"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Spawns a worker for `Topic`. Returns {error, {already_started, Pid}}
%%      if a race caused two callers to try simultaneously.
start_worker(Topic) ->
    ChildSpec = #{
        id       => Topic,
        start    => {service_subscriber_worker, start_link, [Topic]},
        restart  => transient,  % don't restart after a clean stop
        shutdown => 5000,
        type     => worker,
        modules  => [service_subscriber_worker]
    },
    supervisor:start_child(?MODULE, ChildSpec).

init([]) ->
    SupFlags = #{
        strategy  => one_for_one,
        intensity => 10,
        period    => 10
    },
    {ok, {SupFlags, []}}.  % children added dynamically
