%%%-------------------------------------------------------------------
%% @doc mq_xxer top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(mq_xxer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        #{
            id => mq_connector,
            start => {mq_connector, start_link, []},
            restart => permanent,
            shutdown => brutal_kill,
            type => worker,
            modules => [mq_connector]
        },
        #{
            id => mq_consumer_sup,
            start => {mq_consumer_sup, start_link, []},
            restart => permanent,
            shutdown => 2000,
            type => supervisor,
            modules => [mq_consumer_sup]
        },
        #{
            id => mq_producer_sup,
            start => {mq_producer_sup, start_link, []},
            restart => permanent,
            shutdown => 2000,
            type => supervisor,
            modules => [mq_producer_sup]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
