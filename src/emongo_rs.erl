-module(emongo_rs).

-behaviour(gen_server).

-export([start_link/3, pid/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-compile([export_all]).

-include("emongo.hrl").

-record(state, {id,
                master = undefined,
                slaves = [],
                disabled = [],
                timer = undefined
               }).

-define(POOL_ID(BalancerId, PoolIdx), {BalancerId, PoolIdx}).
-define(RECHECK_TIME, 9500).
-define(MASTER_TIME, 1000).
-define(MASTER_OK_TIME, 2000).

%% messages
-define(pid(RequestCount, SlaveOk), {pid, RequestCount, SlaveOk}).

start_link(RsId, Pools, Opts) ->
    gen_server:start_link(?MODULE, [RsId, Pools, Opts], []).

pid(RsPid, RequestCount, SlaveOk) ->
    gen_server:call(RsPid, {pid, RequestCount, SlaveOk}).

init([RsId, Pools, Opts]) ->
    self() ! {init, Pools, Opts},
    {ok, #state{id = RsId}}.

handle_call(?pid(RequestCount, SlaveOk), _From, State) ->
    {Pid, NewState} = get_pid(State, emongo_sup:pools(), RequestCount, SlaveOk),
    {reply, Pid, NewState};

handle_call(stop_children, _, #state{id = RsId, master = Master, slaves = Slaves} = State) ->
    Fun = fun(undefined) ->
                  false;
             (PoolIdx) ->
                  emongo_sup:stop_pool(?POOL_ID(RsId, PoolIdx)),
                  false
          end,
    lists:foreach(Fun, [Master | Slaves]),
    {reply, ok, State#state{master = undefined, slaves = []}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(recheck, State) ->
    {noreply, enable(State)};

handle_info(recheck_master, State) ->
    {_, NewState} = find_master(State),
    {noreply, NewState};

handle_info({init, Pools, Opts}, #state{id = RsId} = State) ->
    Fun = fun({Host, Port, Database, Size}, {PoolIdx, PoolList}) ->
                  case emongo_sup:start_pool(?POOL_ID(RsId, PoolIdx),
                                             Host, Port, Database, Size, Opts) of
                      {ok, _} ->
                          ok;
                      {error, {already_started, _}} ->
                          ok
                  end,
                  {PoolIdx + 1, [PoolIdx | PoolList]}
          end,
    {_, PoolList} = lists:foldl(Fun, {1, []}, Pools),
    
    {_, NewState} = find_master(State#state{slaves = lists:sort(PoolList)}),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%===============================================

find_master(#state{id = RsId, master = Master, slaves = Slaves} = State) ->
    All = case Master of
              undefined ->
                  Slaves;
              _ ->
                  [Master | Slaves]
          end,
    Pools = emongo_sup:pools(),
    case lists:splitwith(fun(P) ->
                             is_master(?POOL_ID(RsId, P), Pools) =:= false
                     end, All) of
        {Slaves1, [NewMaster | Slaves2]} ->
            erlang:send_after(?MASTER_OK_TIME, self(), recheck_master),
            NewState = State#state{master = NewMaster, slaves = lists:usort(Slaves1 ++ Slaves2)},
            if Master =/= NewMaster ->
                    log(NewState, <<"master_changed">>, io_lib:format("\twas=~p", [Master]));
               true ->
                    ok
            end,
            {true, NewState};
        {Slaves1, []} ->
            NewState = State#state{master = undefined, slaves = lists:usort(Slaves1)},
            erlang:send_after(?MASTER_TIME, self(), recheck_master),
            if Master =/= undefined ->
                    log(NewState, <<"master_lost">>, io_lib:format("\twas=~p", [Master]));
               true ->
                    ok
            end,
            {false, NewState}
    end.

is_master(PoolId, Pools) ->
    try emongo_sup:worker_pid(PoolId, Pools, 1, false) of
        undefined ->
            false;
        {Pid, _Database, ReqId} ->
            Packet = emongo_packet:is_master(ReqId),
            case catch emongo_server:send_recv(Pid, ReqId, Packet, 1000) of
                #response{documents = [Doc]} ->
                    case lists:keyfind(<<"ismaster">>, 1, Doc) of
                        {_, true} ->
                            true;
                        _ ->
                            false
                    end;
                _ ->
                    false
            end
    catch _:_ ->
            false
    end.

get_pid(#state{master = undefined} = State, _, _, false) ->
    {undefined, State};
get_pid(#state{id = RsId, master = Master, disabled = Disabled, timer = Timer} = State,
        Pools, RequestCount, false) ->
    case emongo_sup:worker_pid(?POOL_ID(RsId, Master), Pools, RequestCount, false) of
        undefined ->
            error_logger:info_msg("Master pool ~p is disabled!~n", [?POOL_ID(RsId, Master)]),
            TState = State#state{master = undefined,
                                 disabled = [Master | Disabled],
                                 timer = set_timer(Timer)},
            log(TState, <<"master_lost">>, io_lib:format("\twas=~p", [Master])),
            case find_master(TState) of
                {true, NewState} ->
                    get_pid(NewState, Pools, RequestCount, false);
                {false, NewState} ->
                    {undefined, NewState}
            end;
        Res ->
            {Res, State}
    end;

get_pid(#state{slaves = []} = State, _, _, true) ->
    {undefined, State};
get_pid(#state{id = RsId, slaves = [Slave | Next], disabled = Disabled, timer = Timer} = State,
        Pools, RequestCount, true) ->
    case emongo_sup:worker_pid(?POOL_ID(RsId, Slave), Pools, RequestCount, true) of
        undefined ->
            error_logger:info_msg("Slave pool ~p is disabled!~n", [?POOL_ID(RsId, Slave)]),
            NewState = State#state{slaves = Next,
                                   disabled = [Slave | Disabled],
                                   timer = set_timer(Timer)},
            log(NewState, "disabled", io_lib:format("\tpool=~s", [Slave])),
            get_pid(NewState, Pools, RequestCount, true);
        Res ->
            {Res, State}
    end;
    

get_pid(State, Pools, RequestCount, SlaveOk)
  when SlaveOk =:= master_preferred;
       SlaveOk =:= slave_preferred ->
    {First, Next} = if SlaveOk =:= master_preferred ->
                            {false, true};
                       true ->
                            {true, false}
                    end,
    case get_pid(State, Pools, RequestCount, First) of
        {undefined, NewState} ->
            get_pid(NewState, Pools, RequestCount, Next);
        Res ->
            Res
    end;
get_pid(State, _, _, _) ->
    {undefined, State}.


enable(State) ->
    enable(State, [], emongo_sup:pools()).

enable(#state{disabled = [], timer = _TimerRef} = State, [], _) ->
    State#state{timer = undefined};

enable(#state{disabled = []} = State, Disabled, _) ->
    State#state{disabled = Disabled,
                timer = erlang:send_after(?RECHECK_TIME, self(), recheck)};

enable(#state{id = RsId, slaves = Slaves, disabled = [PoolIdx | Disabled]} = State, Acc, Pools) ->
    case emongo_sup:worker_pid(?POOL_ID(RsId, PoolIdx), Pools, 0, false) of
        undefined ->
            enable(State#state{disabled = Disabled}, [PoolIdx | Acc], Pools);
        _ ->
            error_logger:info_msg("pool ~p is enabled!~n", [?POOL_ID(RsId, PoolIdx)]),
            NewState = State#state{slaves = lists:umerge([PoolIdx], Slaves),
                                   disabled = Disabled},
            log(NewState, "enabled", io_lib:format("\tpool=~s", [PoolIdx])),
            enable(NewState, Acc, Pools)
    end.

set_timer(undefined) ->
    erlang:send_after(?RECHECK_TIME, self(), recheck);
set_timer(TimerRef) ->
    TimerRef.

log(#state{id = RsId, master = Master, slaves = Slaves}, Action, Ext) ->
    catch emongo:log_string(
            fun(_) ->
                    io_lib:format("pid=~p\trs=~p\taction=~s\tmaster=~p\tslaves=~p~s",
                                  [self(), RsId, Action, Master, Slaves, Ext])
            end),
    ok.
