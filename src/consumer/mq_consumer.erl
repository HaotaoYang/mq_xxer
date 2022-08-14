%%%-------------------------------------------------------------------
%%% @author
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Aug 2017 下午2:11
%%%-------------------------------------------------------------------
-module(mq_consumer).

-include("mq_xxer_common.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    queue,
    handle_mod,
    opts,
    channel,
    channel_ref
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Args :: term()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init({Queue, HandleMod, Opts}) ->
    NewOpts = #{is_durable := IsDurable} = new_otps(Opts),
    mq_xxer:declare_queue(Queue, IsDurable),    %% 声明队列
    case mq_connector:get_mq_status() of
        true ->
            start_init_channel_timer();
        _ ->
            ?LOG_WARNING("~p:~p: start_consumer mq_status error...~n", [?MODULE, ?LINE]),
            skip
    end,
    {ok, #state{queue = Queue, handle_mod = HandleMod, opts = NewOpts}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
    {reply, unknown_call_msg, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(init_channel, State) ->
    case catch init_channel(State) of
        {ok, NewState} ->
            {noreply, NewState};
        _ ->
            start_init_channel_timer(),
            NewState = State#state{
                channel = undefined,
                channel_ref = undefined
            },
            {noreply, NewState}
    end;
handle_info(#'basic.consume_ok'{}, State) ->
    %% 订阅队列成功返回
    {noreply, State};
handle_info(#'basic.cancel_ok'{} = Cancel, #state{queue = Queue} = State) ->
    ?LOG_WARNING("~p:~p: queue = ~p, ~p~n", [?MODULE, ?LINE, Queue, Cancel]),
    {stop, normal, State};
handle_info({#'basic.deliver'{delivery_tag = Tag} = Deliver, #amqp_msg{payload = Payload} = Content}, State) ->
    #state{handle_mod = HandleMod, channel = Channel} = State,
    case catch HandleMod:handle_msg(Deliver, Content, Payload) of
        {'EXIT', {undef, _}} ->
            ?LOG_WARNING("~p:~p undefined handle_mod = ~p and msg = ~p~n", [?MODULE, ?LINE, HandleMod, Payload]),
            skip;
        _ ->
            skip
    end,
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State};
handle_info({'DOWN', Ref, process, _, Reason}, #state{queue = Queue, channel_ref = Ref} = State) ->
    ?LOG_ERROR("~p:~p: consumer channel down, queue = ~p, reason = ~p~n", [?MODULE, ?LINE, Queue, Reason]),
    NewState = State#state{channel = undefined, channel_ref = undefined},
    start_init_channel_timer(),
    {noreply, NewState};
handle_info(Info, State) ->
    ?LOG_WARNING("~p:~p: unknown info = ~p~n", [?MODULE, ?LINE, Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(Reason, #state{queue = Queue, channel = Channel}) ->
    ?LOG_WARNING("~p:~p: consumer process terminate... queue = ~p, reason = ~p~n", [?MODULE, ?LINE, Queue, Reason]),
    amqp_channel:close(Channel),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
new_otps(Opts) ->
    PrefetchCount = maps:get(prefetch_count, Opts, 2000),
    IsDurable = maps:get(is_durable, Opts, false),
    Opts#{prefetch_count => PrefetchCount, is_durable => IsDurable}.

start_init_channel_timer() ->
    erlang:send_after(1000, self(), init_channel).

init_channel(#state{queue = Queue, opts = #{prefetch_count := PrefetchCount}} = State) ->
    case mq_connector:get_connection() of
        ConnectionPid when is_pid(ConnectionPid) ->
            {ok, Channel} = amqp_connection:open_channel(ConnectionPid),
            #'basic.qos_ok'{} = amqp_channel:call(Channel, #'basic.qos'{prefetch_count = PrefetchCount}),   %% 设置预取值
            #'basic.consume_ok'{} = amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue}, self()),   %% 订阅队列消息
            Ref = erlang:monitor(process, Channel),
            {ok, State#state{channel = Channel, channel_ref = Ref}};
        _ ->
            {error, get_connection_error}
    end.
