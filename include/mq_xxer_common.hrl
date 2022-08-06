-ifndef(MQ_XXER_COMMON_H).
-define(MQ_XXER_COMMON_H, true).

-include_lib("amqp_client/include/amqp_client.hrl").

-define(MQ_XXER_NAME, mq_xxer).
-define(PRODUCER_NUM, 20).  %% 生产者进程总数
-define(CONSUMER_NUM, 20).  %% 每个队列的消费者进程数

-define(LOG_INFO(Format), error_logger:info_msg(Format)).
-define(LOG_WARNING(Format), error_logger:warning_msg(Format)).
-define(LOG_ERROR(Format), error_logger:error_msg(Format)).
-define(LOG_INFO(Format, Args), error_logger:info_msg(Format, Args)).
-define(LOG_WARNING(Format, Args), error_logger:warning_msg(Format, Args)).
-define(LOG_ERROR(Format, Args), error_logger:error_msg(Format, Args)).

-endif.
