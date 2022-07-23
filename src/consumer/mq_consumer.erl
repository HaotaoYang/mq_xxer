%%%-------------------------------------------------------------------
%%% @author
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Aug 2017 下午2:11
%%%-------------------------------------------------------------------
-module(mq_consumer).

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
% -record(state, {ref, conn, queue, channel, mod, mod_state, opt}).
% 
% -type mod_state() :: any().
% -type callback_result() :: {ok, mod_state()} | any().
% -type options() :: [{atom(), any()}].
% -type state() :: #state{}.
% 
% -callback process_init(options()) -> callback_result().
% -callback process_msg(#'basic.deliver'{}, #amqp_msg{}, Channel :: pid(), mod_state()) -> {ok, mod_state()}.
% -callback process_info(Info :: any(), mod_state()) -> {ok, mod_state()}.
% %%%===================================================================
% %%% API
% %%%===================================================================
% %% table column list => consumer_pid, mq_connector_pid, queue_name, channel_pid
% new_ets() ->
%     ?MODULE = ets:new(?MODULE, [set, public, named_table, {read_concurrency, true}]).
% -spec get_worker_state(Queue :: binary()) -> StateList when
%     StateList :: [State],
%     State ::
%     {
%         ConsumerPid :: pid(),
%         ConnectorPid :: pid(),
%         QueueName :: binary(),
%         ChannelPid :: pid()
%     }.
% get_worker_state(Queue) ->
%     M = [{{'_', '_', '$1', '_'}, [{'==', '$1', {const, Queue}}], ['$_']}],
%     ets:select(?MODULE, M).
% -spec start(Queue :: binary(), Counter :: integer(), Mod :: atom(), Opt :: list()) -> ok.
% start(Queue, Counter, Mod, Opt) ->
%     start(Counter, [Queue, Mod, Opt]).
% 
% -spec start(Queue :: binary(), Counter :: integer(), Mod :: atom()) -> ok.
% start(Queue, Counter, Mod) ->
%      start(Counter, [Queue, Mod]).
% 
% start(Counter, Args) ->
%     lists:foreach(fun(_X) ->
%         {ok, _Pid} = supervisor:start_child(spgs_mq_consumer_sup, Args)
%     end, lists:seq(1, Counter)).
% 
% -spec stop(Queue :: binary() | pid()) -> ok.
% stop(Queue) when is_binary(Queue)->
%    [ begin
%          stop(Pid),
%          case erlang:is_process_alive(Channel) of
%              true ->
%               catch amqp_channel:close(Channel);
%              _->
%                  ok
%          end
%      end
%    || {Pid, _Conn, _Queue, Channel} <- get_worker_state(Queue)],
%     ok;
% stop(Pid) when is_pid(Pid) ->
%     case erlang:is_process_alive(Pid) of
%         true ->
%             gen_server:call(Pid, stop);
%         _->
%             ignore
%     end.
% 
% %--------------------------------------------------------------------
% %% @doc
% %% Starts the server
% %%
% %% @end
% %%--------------------------------------------------------------------
% start_link(Queue, Mod) when is_binary(Queue) ->
%     start_link(Queue, Mod, [{prefetch_count, 3000}]).
% -spec(start_link(Queue :: binary(), Mod :: atom(), Opt :: [{Key :: atom(), Value :: atom()}]) ->
%     {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
% start_link(Queue, Mod, Opt) when is_binary(Queue) ->
%     gen_server:start_link(?MODULE, [Queue, Mod, Opt], []).
% 
% %%%===================================================================
% %%% gen_server callbacks
% %%%===================================================================
% 
% %%--------------------------------------------------------------------
% %% @private
% %% @doc
% %% Initializes the server
% %%
% %% @spec init(Args) -> {ok, State} |
% %%                     {ok, State, Timeout} |
% %%                     ignore |
% %%                     {stop, Reason}
% %% @end
% %%--------------------------------------------------------------------
% -spec(init(Args :: term()) ->
%     {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
%     {stop, Reason :: term()} | ignore).
% init([Queue, Mod, Opt]) ->
%     {ok, ModState} = Mod:process_init([{queue, Queue}]),
%     {ok, #state{mod = Mod, opt = Opt, queue = Queue, mod_state = ModState}, 0}.
% 
% %%--------------------------------------------------------------------
% %% @private
% %% @doc
% %% Handling call messages
% %%
% %% @end
% %%--------------------------------------------------------------------
% -spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
%     State :: state()) ->
%     {reply, Reply :: term(), NewState :: state()} |
%     {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
%     {noreply, NewState :: state()} |
%     {noreply, NewState :: state(), timeout() | hibernate} |
%     {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
%     {stop, Reason :: term(), NewState :: state()}).
% handle_call(stop, _, State) ->
%     {stop, normal, ok, State};
% handle_call(_Request, _From, State) ->
%     {reply, ok, State}.
% 
% %%--------------------------------------------------------------------
% %% @private
% %% @doc
% %% Handling cast messages
% %%
% %% @end
% %%--------------------------------------------------------------------
% -spec(handle_cast(Request :: term(), State :: state()) ->
%     {noreply, NewState :: state()} |
%     {noreply, NewState :: state(), timeout() | hibernate} |
%     {stop, Reason :: term(), NewState :: state()}).
% handle_cast(_Request, State) ->
%     {noreply, State}.
% 
% %%--------------------------------------------------------------------
% %% @private
% %% @doc
% %% Handling all non call/cast messages
% %%
% %% @spec handle_info(Info, State) -> {noreply, State} |
% %%                                   {noreply, State, Timeout} |
% %%                                   {stop, Reason, State}
% %% @end
% %%--------------------------------------------------------------------
% -spec(handle_info(Info :: timeout() | term(), State :: state()) ->
%     {noreply, NewState :: state()} |
%     {noreply, NewState :: state(), timeout() | hibernate} |
%     {stop, Reason :: term(), NewState :: state()}).
% handle_info(timeout, #state{opt = Opt, queue = Que} = State) ->
%     try spgs_mq_connector:get_conn() of
%         {ok, Conn} ->
%             case get_channel(Que, Conn, Opt) of
%                 Channel when is_pid(Channel) ->
%                     Ref = erlang:monitor(process, Conn),
%                     erlang:monitor(process, Channel),
%                     NewState = State#state{ref = Ref, conn = Conn, channel = Channel},
%                     save_to_ets(self(), NewState),
%                     {noreply, NewState};
%                 _ ->
%                     erlang:send_after(1000, self(), timeout),
%                     {noreply, State}
%             end;
%         error ->
%             erlang:send_after(1000, self(), timeout),
%             {noreply, State}
%     catch E:R ->
%         lager:error("error=~p,reason=~p", [E, R]),
%         erlang:send_after(1000, self(), timeout),
%         {noreply, State}
%     end;
% 
% handle_info(#'basic.consume_ok'{} = Consume, #state{queue = Que} = State) ->
%     lager:warning("queue=~p, ~p", [Que, Consume]),
%     {noreply, State};
% 
% handle_info(#'basic.cancel_ok'{} = Cancel, State) ->
%     lager:warning("queue=~p, ~p", [State#state.queue, Cancel]),
%     {stop, normal, State};
% 
% handle_info({#'basic.deliver'{} = Deliver, Content}, #state{channel = Channel, mod_state = ModState, mod = Mod} = State) ->
%     try
%         {ok, NewModState} = Mod:process_msg(Deliver, Content, Channel, ModState),
%         {noreply, State#state{mod_state = NewModState}}
%     catch E:R ->
%         lager:error("error=~p,reason=~p", [E, R]),
%         {noreply, State}
%     end;
% 
% handle_info({'DOWN', Ref, process, Pid, _} = Why, #state{ref = Ref, conn = Pid, queue = Q} = State) ->
%     lager:warning("queue=~p, conn = ~p", [Q, Why]),
%     NewState = State#state{conn = undefined, ref = undefined, channel = undefined},
%     save_to_ets(self(), NewState),
%     {noreply, NewState};
% 
% handle_info({'DOWN', _, process, _, _} = Why, #state{queue = Q, ref = _Ref, conn = _Pid} = State) ->
%     lager:warning("queue=~p, chnnale = ~p", [Q, Why]),
%     NewState = State#state{channel = undefined},
%     save_to_ets(self(), NewState),
%     erlang:send_after(1000, self(), timeout),
%     {noreply, NewState};
% 
% handle_info(Info, #state{mod = Mod, mod_state = ModState} = State) ->
%     {ok, NewModState} = Mod:process_info(Info, ModState),
%     {noreply, State#state{mod_state = NewModState}}.
% %%--------------------------------------------------------------------
% %% @private
% %% @doc
% %% This function is called by a gen_server when it is about to
% %% terminate. It should be the opposite of Module:init/1 and do any
% %% necessary cleaning up. When it returns, the gen_server terminates
% %% with Reason. The return value is ignored.
% %%
% %% @spec terminate(Reason, State) -> void()
% %% @end
% %%--------------------------------------------------------------------
% -spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
%     State :: state()) -> term()).
% terminate(_Reason, #state{channel = Channel}) when is_pid(Channel) ->
%     case erlang:is_process_alive(Channel) of
%         true ->
%             amqp_channel:close(Channel);
%         _ ->
%             ok
%     end;
% 
% terminate(_Reason, _) ->
%     ok.
% %%--------------------------------------------------------------------
% %% @private
% %% @doc
% %% Convert process state when code is changed
% %%
% %% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
% %% @end
% %%--------------------------------------------------------------------
% -spec(code_change(OldVsn :: term() | {down, term()}, State :: state(),
%     Extra :: term()) ->
%     {ok, NewState :: state()} | {error, Reason :: term()}).
% code_change(_OldVsn, State, _Extra) ->
%     {ok, State}.
% 
% %%%===================================================================
% %%% Internal functions
% %%%===================================================================
% 
% get_channel(Queue, Conn, Opt) ->
%     Count = proplists:get_value(prefetch_count, Opt),
%     {ok, Channel} = amqp_connection:open_channel(Conn),
%     #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = Queue, durable = true}),
%     #'basic.qos_ok'{} = amqp_channel:call(Channel, #'basic.qos'{prefetch_count = Count}),
%     #'basic.consume_ok'{} = amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue}, self()),
%     Channel.
% save_to_ets(Pid, #state{conn = Conn, queue = Queue, channel = Channel}) ->
%     ets:insert(?MODULE, {Pid, Conn, Queue, Channel}).
% 
% %% do process receive msg
% process_init(_Option) ->
%     {ok, #{}}.
% process_msg(#'basic.deliver'{}, #amqp_msg{}, _Channel, State) ->
%     {ok, State}.
% process_info(_Info, State) ->
%     {ok, State}.
