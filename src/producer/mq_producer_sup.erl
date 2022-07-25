%%%-------------------------------------------------------------------
%% @doc mq_producer_sup top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(mq_producer_sup).

-include("mq_xxer_common.hrl").

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_producers/0,
    stop_producers/0
]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, Pid}.

start_producers() ->
    [{ok, _P} = supervisor:start_child(mq_producer_sup, [N]) || N <- lists:seq(1, ?PRODUCER_NUM)],
    ok.

stop_producers() ->
    [mq_producer:stop_producers(N) || N <- lists:seq(1, ?PRODUCER_NUM)],
    ok.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Child = {
        mq_producer, {mq_producer, start_link, []}, transient, 2000, worker, [mq_producer]
    },
    {ok, {{simple_one_for_one, 10, 60}, [Child]}}.

%%====================================================================
%% Internal functions
%%====================================================================
