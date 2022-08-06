%%%-------------------------------------------------------------------
%% @doc mq_xxer public API
%% @end
%%%-------------------------------------------------------------------

-module(mq_xxer_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    ok = prep_start(),
    case mq_xxer_sup:start_link() of
        {ok, Pid} ->
            post_start(),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    mq_producer_sup:stop_producers(),
    mq_consumer_sup:stop_consumers(),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
prep_start() ->
    ok.

post_start() ->
    mq_producer_sup:start_producers(),
    ok.
