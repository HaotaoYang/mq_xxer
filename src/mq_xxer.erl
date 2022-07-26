-module(mq_xxer).

-include("mq_xxer_common.hrl").

-export([
    declare_exchange/3,
    delete_exchange/1,

    declare_queue/2,
    declare_transient_queue/0,
    delete_queue/1,

    bind/3,
    unbind/3,

    start_consumers/2,
    start_consumers/3
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
                    ?LOG_ERROR("~p:~p: declare_exchange get_connection error~n", [?MODULE, ?LINE]),
                    error
            end;
        _ ->
            ?LOG_ERROR("~p:~p: declare_exchange params error... params:~p~n", [?MODULE, ?LINE, {ExchangeName, ExchangeType, IsDurable}]),
            error
    end.

%% @doc 删除交换机
delete_exchange(ExchangeName) ->
    case check_delete_exchange_params(ExchangeName) of
        true ->
            case mq_connector:get_connection() of
                ConnectionPid when is_pid(ConnectionPid) ->
                    {ok, Channel} = amqp_connection:open_channel(ConnectionPid),
                    ExchangeDelete = #'exchange.delete'{exchange = ExchangeName},
                    #'exchange.delete_ok'{} = amqp_channel:call(Channel, ExchangeDelete),
                    amqp_channel:close(Channel),
                    ok;
                _ ->
                    ?LOG_ERROR("~p:~p: delete_exchange get_connection error~n", [?MODULE, ?LINE]),
                    error
            end;
        _ ->
            ?LOG_ERROR("~p:~p: delete_exchange params error... params:~p~n", [?MODULE, ?LINE, {ExchangeName}]),
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
                    ?LOG_ERROR("~p:~p: declare_queue get_connection error~n", [?MODULE, ?LINE]),
                    error
            end;
        _ ->
            ?LOG_ERROR("~p:~p: declare_queue params error... params:~p~n", [?MODULE, ?LINE, {QueueName, IsDurable}]),
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
            ?LOG_ERROR("~p:~p: declare_queue get_connection error~n", [?MODULE, ?LINE]),
            error
    end.

%% @doc 删除队列
delete_queue(QueueName) ->
    case check_delete_queue_params(QueueName) of
        true ->
            case mq_connector:get_connection() of
                ConnectionPid when is_pid(ConnectionPid) ->
                    {ok, Channel} = amqp_connection:open_channel(ConnectionPid),
                    QueueDelete = #'queue.delete'{queue = QueueName},
                    #'queue.delete_ok'{} = amqp_channel:call(Channel, QueueDelete),
                    amqp_channel:close(Channel),
                    ok;
                _ ->
                    ?LOG_ERROR("~p:~p: delete_queue get_connection error~n", [?MODULE, ?LINE]),
                    error
            end;
        _ ->
            ?LOG_ERROR("~p:~p: delete_queue params error... params:~p~n", [?MODULE, ?LINE, {QueueName}]),
            error
    end.

%% @doc 绑定
bind(QueueName, ExchangeName, RoutingKey) ->
    case check_binding_params(QueueName, ExchangeName, RoutingKey) of
        true ->
            case mq_connector:get_connection() of
                ConnectionPid when is_pid(ConnectionPid) ->
                    {ok, Channel} = amqp_connection:open_channel(ConnectionPid),
                    Bind = #'queue.bind'{
                        queue = QueueName,
                        exchange = ExchangeName,
                        routing_key = RoutingKey
                    },
                    #'queue.bind_ok'{} = amqp_channel:call(Channel, Bind),
                    amqp_channel:close(Channel),
                    ok;
                _ ->
                    ?LOG_ERROR("~p:~p: bind get_connection error~n", [?MODULE, ?LINE]),
                    error
            end;
        _ ->
            ?LOG_ERROR("~p:~p: bind params error... params:~p~n", [?MODULE, ?LINE, {QueueName, ExchangeName, RoutingKey}]),
            error
    end.

%% @doc 解绑
unbind(QueueName, ExchangeName, RoutingKey) ->
    case check_binding_params(QueueName, ExchangeName, RoutingKey) of
        true ->
            case mq_connector:get_connection() of
                ConnectionPid when is_pid(ConnectionPid) ->
                    {ok, Channel} = amqp_connection:open_channel(ConnectionPid),
                    Unbind = #'queue.unbind'{
                        queue = QueueName,
                        exchange = ExchangeName,
                        routing_key = RoutingKey
                    },
                    #'queue.unbind_ok'{} = amqp_channel:call(Channel, Unbind),
                    amqp_channel:close(Channel),
                    ok;
                _ ->
                    ?LOG_ERROR("~p:~p: unbind get_connection error~n", [?MODULE, ?LINE]),
                    error
            end;
        _ ->
            ?LOG_ERROR("~p:~p: unbind params error... params:~p~n", [?MODULE, ?LINE, {QueueName, ExchangeName, RoutingKey}]),
            error
    end.

%% @doc 启动消费者订阅到队列
start_consumers(Queue, HandleMod) ->
    start_consumers(Queue, HandleMod, #{}).

start_consumers(Queue, HandleMod, Opts) ->
    case check_start_consumers_params(Queue, HandleMod, Opts) of
        true ->
            mq_consumer_sup:start_consumers(Queue, HandleMod, Opts);
        _ ->
            ?LOG_ERROR("~p:~p: start_consumers error... params:~p~n", [?MODULE, ?LINE, {Queue, HandleMod, Opts}]),
            error
    end.

%%%===================================================================
%%% Internal functions
%%===================================================================
check_declare_exchange_params(ExchangeName, ExchangeType, IsDurable) ->
    is_binary(ExchangeName) andalso is_binary(ExchangeType) andalso is_boolean(IsDurable).

check_delete_exchange_params(ExchangeName) -> is_binary(ExchangeName).

check_declare_queue_params(QueueName, IsDurable) ->
    is_binary(QueueName) andalso is_boolean(IsDurable).

check_delete_queue_params(QueueName) -> is_binary(QueueName).

check_binding_params(QueueName, ExchangeName, RoutingKey) ->
    is_binary(QueueName) andalso is_binary(ExchangeName) andalso is_binary(RoutingKey).

check_start_consumers_params(Queue, HandleMod, Opts) ->
    is_binary(Queue) andalso is_atom(HandleMod) andalso is_map(Opts).
