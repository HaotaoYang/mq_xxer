-module(mq_connector).

-include("mq_xxer_common.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {
    connection,
    monitor_ref
}).

-export([
    start_link/0,
    get_connection/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%====================================================================
%% API
%%====================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_connection() -> pid() | undefined.
get_connection() ->
    gen_server:call(?MODULE, get_connection).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init(_) ->
    State = case catch start_mq_client() of
        {Connection, Ref} -> #state{connection = Connection, monitor_ref = Ref};
        _ -> #state{}
    end,
    {ok, State}.

handle_call(get_connection, _From, State) ->
    {reply, State#state.connection, State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    NewState = case catch start_mq_client() of
        {Connection, Ref} ->
            State#state{connection = Connection, monitor_ref = Ref};
        _ ->
            erlang:send_after(1000, self(), tick),
            State
    end,
    {noreply, NewState};
handle_info({'DOWN', Ref, process, Pid, _}, #state{connection = Pid, monitor_ref = Ref} = State) ->
    erlang:send_after(1000, self(), tick),
    {noreply, State};
handle_info(Info, State) ->
    io:format("~p:~p: [info] mq_connector process receive unknow info msg:~p~n", [?MODULE, ?LINE, Info]),
    {noreply, State}.

terminate(Reason, _State) ->
    io:format("~p:~p: [warning] mq_connector process terminate... reason:~p~n", [?MODULE, ?LINE, Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
start_mq_client() ->
    case connect_mq() of
        {ok, Connection} ->
            Ref = erlang:monitor(process, Connection),
            {Connection, Ref};
        _ ->
            skip
    end.

connect_mq() ->
    case application:get_env(?MQ_XXER_NAME, mq_cfg) of
        {ok, MqCfg} ->
            IsMqStart = proplists:get_value(is_mq_start, MqCfg, false),
            Username = proplists:get_value(username, MqCfg, <<"guest">>),
            Password = proplists:get_value(password, MqCfg, <<"guest">>),
            Host = proplists:get_value(host, MqCfg, "127.0.0.1"),
            Port = proplists:get_value(port, MqCfg, 5672),
            VHost = proplists:get_value(virtual_host, MqCfg, <<"/">>),
            case IsMqStart of
                true ->
                    Params = #amqp_params_network{
                        username = to_binary(Username),
                        password = to_binary(Password),
                        host = to_list(Host),
                        port = to_integer(Port),
                        virtual_host = to_binary(VHost)
                    },
                    amqp_connection:start(Params);
                _ -> skip
            end;
        _ -> skip
    end.

to_binary(V) when is_binary(V) ->
    V;
to_binary(V) when is_list(V) ->
    erlang:list_to_binary(V).
to_integer(V) when is_integer(V) ->
    V;
to_integer(V) when is_list(V) ->
    erlang:list_to_integer(V).
to_list(V) when is_list(V) ->
    V;
to_list(V) when is_binary(V) ->
    erlang:binary_to_list(V).
