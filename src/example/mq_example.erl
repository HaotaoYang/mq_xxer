-module(mq_example).

-include("mq_xxer_common.hrl").

-export([
    start/0,
    handle_msg/3
]).

%%%===================================================================
%%% API
%%%===================================================================
start() ->
    ExchangeName = <<"direct_exchange">>,
    ExchangeType = <<"direct">>,
    IsDurable = false,
    QueueName1 = <<"direct_queue1">>,
    RoutingKey1 = <<"direct_routing_key1">>,
    QueueName2 = <<"direct_queue2">>,
    RoutingKey2 = <<"direct_routing_key2">>,
    mq_xxer:declare_exchange(ExchangeName, ExchangeType, IsDurable),    %% 声明交换机
    mq_xxer:declare_queue(QueueName1, IsDurable),                       %% 声明队列
    mq_xxer:declare_queue(QueueName2, IsDurable),                       %% 声明队列
    mq_xxer:bind(QueueName1, ExchangeName, RoutingKey1),                %% 队列与交换机绑定
    mq_xxer:bind(QueueName2, ExchangeName, RoutingKey2),                %% 队列与交换机绑定
    mq_xxer:start_consumers(QueueName1, ?MODULE),
    mq_xxer:start_consumers(QueueName2, ?MODULE),
    spawn(fun() -> publish_loop(ExchangeName, [RoutingKey1, RoutingKey2], 0) end).

handle_msg(#'basic.deliver'{exchange = ExchangeName, routing_key = RoutingKey}, _Content, Msg) ->
    ?LOG_INFO("~p:~p: handle_msg... exchange = ~p, routing_key = ~p, msg = ~p~n", [?MODULE, ?LINE, ExchangeName, RoutingKey, Msg]),
    ok.

%%%===================================================================
%%% Internal functions
%%===================================================================
publish_loop(ExchangeName, RoutingKeyList, MsgNum) ->
    NewMsgNum = MsgNum + 1,
    RoutingKey = lists:nth(rand:uniform(length(RoutingKeyList)), RoutingKeyList),
    mq_producer:publish({ExchangeName, RoutingKey, erlang:integer_to_binary(NewMsgNum)}),
    timer:sleep(500),
    publish_loop(ExchangeName, RoutingKeyList, NewMsgNum).


