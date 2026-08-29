%% @doc Root supervisor. Starts children in dependency order:
%%      metrics → worker_sup → db pool (DB_POOL_SIZE workers) →
%%      readers (READ_POOL_SIZE, only when READS_PER_SEC > 0) → mqtt client.
%%
%% The readers are an independent branch: nothing in the ingest path routes to
%% them and they hold no per-topic state, so they start before the MQTT client
%% and generate load whether or not any sensor is publishing.
-module(service_subscriber_sup).
-behaviour(supervisor).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%% Registers the supervisor locally and starts it under the OTP application.
start_link() ->
    ?LOG_INFO("Subscriber Supervisor Started"),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% Builds child specs for metrics, worker supervisor, DB pool, readers, and MQTT client in
%% dependency order.
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

    %% Artificial read load, sized by READ_POOL_SIZE and paced by READS_PER_SEC. reader_count/0
    %% returns 0 when READS_PER_SEC is 0, so the default configuration builds no reader children at
    %% all -- no processes, no timers, and no PostgreSQL connections beyond the DB pool's.
    Readers = [
        #{id      => {service_subscriber_reader, I},
          start   => {service_subscriber_reader, start_link, [I]},
          restart => permanent,
          shutdown => 5000,
          type    => worker,
          modules => [service_subscriber_reader]}
        || I <- lists:seq(1, service_subscriber_reader:reader_count())
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
    ] ++ DBWorkers ++ Readers ++ [
        #{id      => service_subscriber_mqtt,
          start   => {service_subscriber_mqtt, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type    => worker,
          modules => [service_subscriber_mqtt]}
    ],
    {ok, {SupFlags, ChildSpecs}}.
