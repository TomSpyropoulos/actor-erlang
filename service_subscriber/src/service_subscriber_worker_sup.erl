-module(service_subscriber_worker_sup).
-behaviour(supervisor).

-export([start_link/0, start_worker/1, init/1]).

start_link() ->
    io:format("Worker Supervisor Started~n"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_worker(Topic) ->
    ChildSpec = #{
        id => Topic,
        start => {service_subscriber_worker, start_link, [Topic]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [service_subscriber_worker]
    },
    supervisor:start_child(?MODULE, ChildSpec).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, []}}.
