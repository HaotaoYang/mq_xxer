%%%-------------------------------------------------------------------
%% @doc mq_consumer_sup top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(mq_consumer_sup).

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
    {ok, Pid} = supervisor:start_link({local, ?SERVER}, ?MODULE, []),
    {ok, Pid}.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Child = {
        mq_consumer, {mq_consumer, start_link, []}, transient, 2000, worker, [mq_consumer]
    },
    {ok, {{simple_one_for_one, 10, 60}, [Child]}}.

%%====================================================================
%% Internal functions
%%====================================================================
