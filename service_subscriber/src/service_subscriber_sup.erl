%% @doc Root supervisor. Starts children in dependency order:
%%      metrics → worker_sup → db pool (DB_POOL_SIZE workers) → mqtt client.
-module(service_subscriber_sup).
-behaviour(supervisor).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    ?LOG_INFO("Subscriber Supervisor Started"),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy  => one_for_one,
        intensity => 3,
        period    => 5
    },

    service_subscriber_db:init_counter(),

    %% Pool size is set via DB_POOL_SIZE env var (default 20).
    %% init_counter/0 stores it in persistent_term; read the same var here
    %% to build the child spec list consistently.
    PoolSize = list_to_integer(os:getenv("DB_POOL_SIZE", "20")),

    %% One GenServer per DB connection; writes are distributed via round-robin.
    DBWorkers = [
        #{id      => {service_subscriber_db, I},
          start   => {service_subscriber_db, start_link, [I]},
          restart => permanent,
          shutdown => 5000,
          type    => worker,
          modules => [service_subscriber_db]}
        || I <- lists:seq(1, PoolSize)
    ],

    ChildSpecs = [
        #{id      => service_subscriber_metrics,
          start   => {service_subscriber_metrics, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type    => worker,
          modules => [service_subscriber_metrics]},
        #{id      => service_subscriber_worker_sup,
          start   => {service_subscriber_worker_sup, start_link, []},
          restart => permanent,
          shutdown => infinity,
          type    => supervisor,
          modules => [service_subscriber_worker_sup]}
    ] ++ DBWorkers ++ [
        #{id      => service_subscriber_mqtt,
          start   => {service_subscriber_mqtt, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type    => worker,
          modules => [service_subscriber_mqtt]}
    ],
    {ok, {SupFlags, ChildSpecs}}.
