-module(mq_xxer).

-include("mq_xxer_common.hrl").

-export([
    declare_exchange/3,
    declare_queue/2,
    declare_transient_queue/0
]).

%%%===================================================================
%%% API
%%%===================================================================
%% @doc 声明交换机
declare_exchange(ExchangeName, ExchangeType, IsDurable) ->
    case check_declare_exchange_params(ExchangeName, ExchangeType, IsDurable) of
        true ->
            case mq_connector:get_connection() of
                ConnectionPid when is_pid(ConnectionPid) ->
                    {ok, Channel} = amqp_connection:open_channel(ConnectionPid),
                    ExchangeDeclare = #'exchange.declare'{
                        exchange = ExchangeName,
                        type = ExchangeType,
                        durable = IsDurable
                    },
                    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
                    amqp_channel:close(Channel),
                    ok;
                _ ->
                    ?LOG_ERROR("declare_exchange get_connection error~n"),
                    error
            end;
        _ ->
            ?LOG_ERROR("declare_exchange params error... params:~p~n", [{ExchangeName, ExchangeType, IsDurable}]),
            error
    end.

%% @doc 声明队列
declare_queue(QueueName, IsDurable) ->
    case check_declare_queue_params(QueueName, IsDurable) of
        true ->
            case mq_connector:get_connection() of
                ConnectionPid when is_pid(ConnectionPid) ->
                    {ok, Channel} = amqp_connection:open_channel(ConnectionPid),
                    QueueDeclare = #'queue.declare'{
                        queue = QueueName,
                        durable = IsDurable
                    },
                    #'queue.declare_ok'{} = amqp_channel:call(Channel, QueueDeclare),
                    amqp_channel:close(Channel),
                    ok;
                _ ->
                    ?LOG_ERROR("declare_queue get_connection error~n"),
                    error
            end;
        _ ->
            ?LOG_ERROR("declare_queue params error... params:~p~n", [{QueueName, IsDurable}]),
            error
    end.

%% @doc 声明临时队列，返回唯一的队列名字
declare_transient_queue() ->
    case mq_connector:get_connection() of
        ConnectionPid when is_pid(ConnectionPid) ->
            {ok, Channel} = amqp_connection:open_channel(ConnectionPid),
            QueueDeclare = #'queue.declare'{},
            #'queue.declare_ok'{queue = QueueName} = amqp_channel:call(Channel, QueueDeclare),
            amqp_channel:close(Channel),
            {ok, QueueName};
        _ ->
            ?LOG_ERROR("declare_queue get_connection error~n"),
            error
    end.

%%%===================================================================
%%% Internal functions
%%===================================================================
check_declare_exchange_params(ExchangeName, ExchangeType, IsDurable) ->
    is_binary(ExchangeName) andalso is_binary(ExchangeType) andalso is_boolean(IsDurable).

check_declare_queue_params(QueueName, IsDurable) ->
    is_binary(QueueName) andalso is_boolean(IsDurable).
