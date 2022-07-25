-module(mq_producer).

-include("mq_xxer_common.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    get_name/1,
    % send/1,
    % state/1,
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

% -type publish_msg() :: {Exchange :: binary(), RoutingKey :: binary(), Payload :: binary()} |
%                         {#'basic.publish'{}, #amqp_msg{}}.
% -type result() :: ack | nack | closing | blocked.

-record(state, {
    channel,
    channel_ref,
    request = dict:new(),
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

% -spec send( Msg :: publish_msg()) -> Result when
%     Result :: result().
% send(Msg) ->
%    send(whereis(get_publisher()), Msg).
%
% get_publisher() ->
%     N = rand:uniform(?PRODUCER_NUM),
%     get_name(N).
% 
% -spec send(Pid, Msg :: publish_msg()) -> Result when
%     Pid :: pid(),
%     Result :: result().
% send(Pid, Msg) when
%     is_pid(Pid)
%          ->
%     gen_server:call(Pid, {send, Msg}, infinity).
% 
% -spec state(Pid :: pid()) -> #state{}.
% state(Pid) ->
%     sys:get_state(Pid).

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
init([_PName]) ->
    case mq_connector:get_mq_status() of
        true ->
            start_init_channel_timer();
        _ ->
            skip
    end,
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
% handle_call({send, _}, _From, #state{channel = undefined} = NewState) ->
%     {reply, closing, NewState};
% handle_call({send, {Exchange, RoutingKey, Payload}},  From,
%     State) ->
%         BasicPublish = #'basic.publish'{
%         mandatory = true,
%         exchange = Exchange,
%         routing_key = RoutingKey},
%         AmqpMsg =  #amqp_msg{props = #'P_basic'{
%         content_type = <<"application/octet-stream">>,
%         timestamp = erlang:system_time(1000),
%         delivery_mode = 2},
%         payload = Payload
%        },
%     handle_call({send, {BasicPublish, AmqpMsg}}, From ,State);
% handle_call({send, {#'basic.publish'{} = Publish, #amqp_msg{props = Props} = AmqpMsg}}, From,
%     #state{channel = Channel, seqno = SeqNo, request = Req} = State) ->
%     NewSeq = SeqNo + 1,
%     NewProps = Props#'P_basic'{message_id = erlang:integer_to_binary(NewSeq)},
%     case catch amqp_channel:call(Channel, Publish, AmqpMsg#amqp_msg{props = NewProps}) of
%         ok ->
%             NewReq = dict:store(NewSeq, From, Req),
%             {noreply, State#state{seqno = NewSeq, request = NewReq}};
%         %% blocked or closing
%         BlockedOrClosing ->
%             {reply, BlockedOrClosing, State}
%     end;

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
                request = dict:new(),
                seqno = 0
            },
            {noreply, NewState}
    end;

% %% @doc when no queue to binding this routingkey will callback
% handle_info({#'basic.return'{} = Return, #amqp_msg{props = #'P_basic'{message_id = SeqNo
%     }} = _AmqpMsg},
%     #state{request = Req} = State) ->
%     ?LOG_ERROR("pub_pid = ~p, callback_return = ~p~n", [self(), Return]),
%     NewR = do_response(erlang:binary_to_integer(SeqNo), nack, Req),
%     {noreply, State#state{request = NewR}};
% handle_info(#'basic.ack'{delivery_tag = SeqNo, multiple = Multiple}, #state{request = Req} = State) ->
%     {noreply, State#state{request = do_response({SeqNo, Multiple}, ack, Req)}};
% handle_info(#'basic.nack'{delivery_tag = SeqNo, multiple = Multiple}, #state{request = Req} = State) ->
%     {noreply, State#state{request = do_response({SeqNo, Multiple}, nack, Req)}};
%% @doc publish message to no exchange will receive down from channel
handle_info({'DOWN', _, process, QPid, {shutdown,{server_initiated_close, 404, _}} = Reason},
    #state{request = Req} = State) ->
    ?LOG_ERROR("NOF_FOUND pub_pid = ~p, channel_pid(~p) down, reason = ~p~n", [self(), QPid, Reason]),
    dict:fold(fun channel_down/3, ok, Req),
    start_init_channel_timer(),
    {noreply, State};
handle_info({'DOWN', _, process, QPid, Reason}, #state{request = Req} = State) ->
    ?LOG_ERROR("pub_pid = ~p, channel_pid(~p) down, reason = ~p~n", [self(), QPid, Reason]),
    dict:fold(fun channel_down/3, ok, Req),
    start_init_channel_timer(),
    {noreply, State};
handle_info(Info, State) ->
    ?LOG_WARNING("self = ~p, unknown info = ~p~n", [self(), Info]),
    {noreply, State}.

channel_down(_, From, Acc) ->
    gen_server:reply(From, nack),
    Acc.

% do_response({SeqNo, true}, Reply, Req) ->
%     %% 小于SeqNo的消息都已经确认
%     {NewReq, _, _} = dict:fold(fun do_multiple_response/3, {dict:new(), SeqNo, Reply}, Req),
%     NewReq;
% 
% do_response({SeqNo, false}, Reply, Req) ->
%     do_response(SeqNo, Reply, Req);
% do_response(SeqNo, Reply, Req) ->
%     case dict:find(SeqNo, Req) of
%         {ok, From} ->
%             gen_server:reply(From, Reply),
%             dict:erase(SeqNo, Req);
%         error ->
%             Req
%     end.
% 
% do_multiple_response(SeqNo, From, {Remain, UpSeqNo, Reply}) ->
%     case SeqNo > UpSeqNo of
%         true ->
%             {dict:store(SeqNo, From, Remain), UpSeqNo, Reply};
%         false ->
%             gen_server:reply(From, Reply),
%             {Remain, UpSeqNo, Reply}
%     end.
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
    dict:fold(fun channel_down/3, ok, Req),
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
            ok = amqp_channel:register_confirm_handler(Channel, self()),
            ok = amqp_channel:register_return_handler(Channel, self()),
            #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
            Ref = erlang:monitor(process, Channel),
            {ok, State#state{channel = Channel, channel_ref = Ref, seqno = 0, request = dict:new()}};
        _ ->
            {error, get_connection_error}
    end.
