%%%-------------------------------------------------------------------
%% @doc mq_consumer_sup top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(mq_consumer_sup).

-include("mq_xxer_common.hrl").

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_consumers/3,
    stop_consumers/0
]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, Pid}.

start_consumers(Queue, HandleMod, Opts) ->
    [supervisor:start_child(mq_consumer_sup, [{Queue, HandleMod, Opts}]) || _N <- lists:seq(1, ?CONSUMER_NUM)],
    ok.

stop_consumers() -> ok.

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
