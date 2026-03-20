-module(service_subscriber_sup).
-moduledoc """
service_subscriber top level supervisor.
""".

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
	io:format("Supervisor Started~n"),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 3,
        period => 5
    },
    ChildSpecs = [
        #{id => service_subscriber_mqtt,
          start => {service_subscriber_mqtt, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [service_subscriber_mqtt]}
    ],
    {ok, {SupFlags, ChildSpecs}}.

% functions
