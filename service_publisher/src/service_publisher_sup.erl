-module(service_publisher_sup).
-moduledoc """
service_publisher top level supervisor.
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
        #{id => service_publisher_srv,
          start => {service_publisher_srv, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [service_publisher_srv]}
    ],
    {ok, {SupFlags, ChildSpecs}}.

% functions
