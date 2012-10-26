-module(emongo_sup).

-behaviour(supervisor).

-export([start_link/0, pools/0, worker_pid/4]).
-export([start_pool/5, start_pool/6, stop_pool/1]).
-export([start_router/2, stop_router/1]).
-export([start_rs/2, start_rs/3, stop_rs/1]).

%% supervisor exports
-export([init/1]).

%%%%%%%%%%%%%%%%
%% public api %%
%%%%%%%%%%%%%%%%

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_router(BalId, Pools) ->
    supervisor:start_child(?MODULE,
                           {BalId,
                            {emongo_router, start_link, [BalId, Pools]},
                            permanent, 10000, worker, [emongo_router]
                           }).

stop_router(BalId) ->
    case [Pid || {PoolId, Pid, _, [emongo_router]} <- supervisor:which_children(?MODULE), PoolId =:= BalId] of
        [Pid] ->
            gen_server:call(Pid, stop_children),
            stop_pool(BalId)
    end.

start_rs(RsId, Pools) ->
    start_rs(RsId, Pools, []).

start_rs(RsId, Pools, Opts) ->
    supervisor:start_child(?MODULE,
                           {RsId,
                            {emongo_rs, start_link, [RsId, Pools, Opts]},
                            permanent, 10000, worker, [emongo_rs]
                           }).

stop_rs(RsId) ->
    case [Pid || {PoolId, Pid, _, [emongo_rs]} <- supervisor:which_children(?MODULE), PoolId =:= RsId] of
        [Pid] ->
            gen_server:call(Pid, stop_children),
            stop_pool(RsId)
    end.


start_pool(PoolId, Host, Port, Database, Size) ->
    start_pool(PoolId, Host, Port, Database, Size, []).

start_pool(PoolId, Host, Port, Database, Size, Opts) ->
    supervisor:start_child(?MODULE, {PoolId,
		{emongo_pool, start_link, [PoolId, Host, Port, Database, Size, Opts]},
		permanent, 10000, worker, [emongo_pool]
	}).


stop_pool(PoolPid) when is_pid(PoolPid) ->
    case [PoolId || {PoolId, Pid,_,_} <- supervisor:which_children(?MODULE), Pid =:= PoolPid] of
     	[PoolId] -> stop_pool(PoolId);
     	_ -> {error, not_found}
    end;

stop_pool(PoolId) ->
    supervisor:terminate_child(?MODULE, PoolId),
    supervisor:delete_child(?MODULE, PoolId).


pools() ->
    [{Id, Pid, Module} || {Id, Pid, _, [Module]}
                              <- supervisor:which_children(?MODULE), Module /= emongo].


worker_pid(PoolPid, Pools, RequestCount, SlaveOk) when is_pid(PoolPid) ->
    case lists:keyfind(PoolPid, 2, Pools) of
        {_, Pid, Module} ->
            Module:pid(Pid, RequestCount, SlaveOk);
        _ ->
            undefined
    end;

worker_pid(PoolId, Pools, RequestCount, SlaveOk) ->
    case lists:keyfind(PoolId, 1, Pools) of
        {_, Pid, Module} ->
            Module:pid(Pid, RequestCount, SlaveOk);
        _ ->
            undefined
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%
%% supervisor callbacks %%
%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_) ->
	{ok, {{one_for_one, 10, 10}, [
		{emongo, {emongo, start_link, []},
		 permanent, 5000, worker, [emongo]}
	]}}.
