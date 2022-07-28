-module(mq_xxer).

-include("mq_xxer_common.hrl").

-export([
    declare_exchange/3
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
                    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare);
                _ ->
                    ?LOG_ERROR("declare_exchange get_connection error~n")
            end;
        _ ->
            ?LOG_ERROR("declare_exchange params error... params:~p~n", [{ExchangeName, ExchangeType, IsDurable}])
    end.

%%%===================================================================
%%% Internal functions
%%===================================================================
check_declare_exchange_params(ExchangeName, ExchangeType, IsDurable) ->
    is_binary(ExchangeName) andalso is_binary(ExchangeType) andalso is_boolean(IsDurable).
