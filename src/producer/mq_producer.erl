-module(mq_producer).

% -behaviour(gen_server).
% %% API
% -export([start_link/2]).
% -export([start_link/3]).
% -export([new_ets/0]).
% -export([get_worker_state/1]).
% -export([start/3]). %% 启动consumer入口函数
% -export([start/4]). %%
% -export([stop/1]). %% 关闭consumer
% -export([process_init/1]).
% -export([process_info/2]).
% -export([process_msg/4]).
% -include_lib("amqp_client/include/amqp_client.hrl").
% 
% %% gen_server callbacks
% -export([init/1,
%     handle_call/3,
%     handle_cast/2,
%     handle_info/2,
%     terminate/2,
%     code_change/3]).
% 
% -define(SERVER, ?MODULE).
