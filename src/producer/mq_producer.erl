-module(mq_producer).

-include("mq_xxer_common.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    get_name/1,
    state/1,
    publish/1,
    stop_producers/1
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

-type publish_msg() :: {Exchange :: binary(), RoutingKey :: binary(), Payload :: binary()}.
-type result() :: ack | nack | closing | blocked.

-record(state, {
    name,
    channel,
    channel_ref,
    request = [],
    seqno = 0   %% 当前channel消息序号
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
-spec(start_link(N :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(N) when is_integer(N) ->
    PName = get_name(N),
    gen_server:start_link({local, get_name(N)}, ?MODULE, [PName], []).

get_name(N) ->
    erlang:list_to_atom(lists:concat([?MODULE, "_", N])).

-spec state(Name :: atom()) -> #state{}.
state(PName) ->
    sys:get_state(PName).

-spec publish(Msg :: publish_msg()) -> Result :: result().
publish(Msg) ->
    N = rand:uniform(?PRODUCER_NUM),
    PName = get_name(N),
    publish(PName, Msg).

-spec publish(Name :: atom(), Msg :: publish_msg()) -> Result :: result().
publish(PName, Msg) ->
    gen_server:cast(PName, {publish, Msg}, infinity).

stop_producers(N) ->
    gen_server:call(get_name(N), stop).

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
init([PName]) ->
    case mq_connector:get_mq_status() of
        true ->
            start_init_channel_timer();
        _ ->
            skip
    end,
    {ok, #state{name = PName}}.

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
handle_call(stop, _, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, unknown_proto, State}.

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
handle_cast({publish, _}, #state{channel = undefined} = NewState) ->
    {reply, closing, NewState};
handle_cast({publish, {Exchange, RoutingKey, Payload} = Msg}, State) ->
    #state{channel = Channel, seqno = SeqNo, request = Req} = State,
    NewSeq = SeqNo + 1,
    BasicPublish = #'basic.publish'{
        mandatory = true,   %% 消息不能路由到任何一个队列时(true:通过#'basic.return'返回给生产者 | false:消息将被丢弃)
        exchange = Exchange,
        routing_key = RoutingKey
    },
    AmqpMsg = #amqp_msg{
        props = #'P_basic'{
            delivery_mode = 2,  %% 持久化参数(1:临时的 | 2:持久化的)
            timestamp = erlang:system_time(1000),
            message_id = erlang:integer_to_binary(NewSeq)
        },
        payload = Payload
    },
    case catch amqp_channel:call(Channel, BasicPublish, AmqpMsg) of
        ok ->
            NewReq = Req ++ [{NewSeq, Msg}],
            {noreply, State#state{seqno = NewSeq, request = NewReq}};
        %% blocked or closing
        BlockedOrClosing ->
            handle_confirm_msg(BlockedOrClosing, Msg),
            {noreply, State}
    end;
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
                channel_ref = undefined,
                channel = undefined,
                request = [],
                seqno = 0
            },
            {noreply, NewState}
    end;
%% @doc when no queue to binding this routingkey will callback
handle_info({#'basic.return'{} = Return, #amqp_msg{props = #'P_basic'{message_id = SeqNo}}}, State) ->
    #state{name = Name, request = Req} = State,
    ?LOG_ERROR("~p:~p: producer_name = ~p, callback_return = ~p~n", [?MODULE, ?LINE, Name, Return]),
    {noreply, State#state{request = handle_publish_confirm(erlang:binary_to_integer(SeqNo), nack, Req)}};
handle_info(#'basic.ack'{delivery_tag = SeqNo, multiple = Multiple}, #state{request = Req} = State) ->
    {noreply, State#state{request = handle_publish_confirm({SeqNo, Multiple}, ack, Req)}};
handle_info(#'basic.nack'{delivery_tag = SeqNo, multiple = Multiple}, #state{request = Req} = State) ->
    {noreply, State#state{request = handle_publish_confirm({SeqNo, Multiple}, nack, Req)}};
handle_info({'DOWN', _, process, _ChannelPid, Reason}, #state{name = Name, request = Req} = State) ->
    ?LOG_ERROR("~p:~p: producer_name = ~p down, reason = ~p~n", [?MODULE, ?LINE, Name, Reason]),
    do_channel_down(Req),
    start_init_channel_timer(),
    {noreply, State};
handle_info(Info, State) ->
    ?LOG_WARNING("self = ~p, unknown info = ~p~n", [self(), Info]),
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
terminate(_Reason, #state{channel = Channel, request = Req}) ->
    do_channel_down(Req),
    amqp_channel:unregister_confirm_handler(Channel),
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
%%===================================================================
start_init_channel_timer() ->
    erlang:send_after(1000, self(), init_channel).

init_channel(State) ->
    case mq_connector:get_connection() of
        ConnectionPid when is_pid(ConnectionPid) ->
            {ok, Channel} = amqp_connection:open_channel(ConnectionPid),
            #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),   %% 启用发布确认
            ok = amqp_channel:register_confirm_handler(Channel, self()),    %% (发布确认回调函数)异步确认方式
            ok = amqp_channel:register_return_handler(Channel, self()),
            Ref = erlang:monitor(process, Channel),
            {ok, State#state{channel = Channel, channel_ref = Ref, seqno = 0, request = []}};
        _ ->
            {error, get_connection_error}
    end.

handle_publish_confirm({SeqNo, true}, Reply, Req) ->
    %% 批量确认，小于等于SeqNo的消息都已经确认
    NewReq = lists:foldl(
        fun({TempSeqNo, Msg}, TempReq) ->
            case TempSeqNo =< SeqNo of
                true ->
                    handle_confirm_msg(Reply, Msg),
                    TempReq;
                _ ->
                    TempReq ++ [{TempSeqNo, Msg}]
            end
        end,
        [],
        Req
    ),
    NewReq;
handle_publish_confirm({SeqNo, false}, Reply, Req) ->
    handle_publish_confirm(SeqNo, Reply, Req);
handle_publish_confirm(SeqNo, Reply, Req) ->
    case lists:keytake(SeqNo, 1, Req) of
        {value, {SeqNo, Msg}, Other} ->
            handle_confirm_msg(Reply, Msg),
            Other;
        _ ->
            Req
    end.

do_channel_down(Req) ->
    [handle_confirm_msg(nack, Msg) || {_, Msg} <- Req].

handle_confirm_msg(ack, _Msg) -> skip;
handle_confirm_msg(Reply, Msg) ->
    %% TODO handle_msg
    ?LOG_WARNING("~p:~p: msg ~p confirm response:~p~n", [?MODULE, ?LINE, Msg, Reply]),
    ok.
