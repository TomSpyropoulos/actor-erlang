%% @doc The SQLite write lock: one per node, shared by every db_backend_sqlite pool worker. SQLite
%% allows one writer, so a worker takes this before BEGIN rather than waiting for the file lock in
%% SQLite's busy handler, which holds a dirty IO scheduler while it sleeps. Without it, enough
%% sleeping writers starve the lock holder of a scheduler and the pool deadlocks at a large
%% DB_POOL_SIZE. Callers queue in arrival order, and a holder that dies releases the lock through
%% its monitor. Mirrors SQLiteBackend.withWriteLock.
-module(db_backend_sqlite_lock).
-behaviour(gen_server).

-export([start_link/0, acquire/0, release/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% State of the lock.
%% `holder`: {Pid, MonitorRef} of the current holder, or undefined when free.
%% `waiting`: Callers blocked in acquire/0, oldest first.
-record(lock, {
    holder  :: {pid(), reference()} | undefined,
    waiting :: queue:queue(gen_server:from())
}).

%% --- API Functions ---

%% Registered locally, so workers find it without being handed a pid.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Blocks until the caller holds the lock. No timeout: a caller only waits while another write runs,
%% and busy_timeout still bounds how long that write can wait on the file.
acquire() ->
    gen_server:call(?MODULE, acquire, infinity).

%% Hands the lock to the next waiter, or frees it.
release() ->
    gen_server:call(?MODULE, release, infinity).

%% --- gen_server Callbacks ---

%% Starts free with nobody waiting.
init([]) ->
    {ok, #lock{holder = undefined, waiting = queue:new()}}.

%% Grants the lock at once when free and queues the caller otherwise. Only the holder can release.
handle_call(acquire, {Pid, _} = From, #lock{holder = undefined} = L) ->
    gen_server:reply(From, ok),
    {noreply, L#lock{holder = {Pid, monitor(process, Pid)}}};
handle_call(acquire, From, #lock{waiting = W} = L) ->
    {noreply, L#lock{waiting = queue:in(From, W)}};
handle_call(release, {Pid, _}, #lock{holder = {Pid, Ref}} = L) ->
    demonitor(Ref, [flush]),
    {reply, ok, grant_next(L)};
handle_call(release, _From, L) ->
    {reply, {error, not_holder}, L}.

%% No casts are used. Satisfy the callback contract.
handle_cast(_Msg, L) ->
    {noreply, L}.

%% A holder that died mid-write never releases, so its monitor does it. SQLite has already rolled the
%% transaction back by then, since the connection closed with its owner.
handle_info({'DOWN', Ref, process, _, _}, #lock{holder = {_, Ref}} = L) ->
    {noreply, grant_next(L)};
handle_info(_Msg, L) ->
    {noreply, L}.

%% --- Internal helpers ---

%% Passes the lock to the oldest live waiter. A waiter that died while queued is skipped rather than
%% granted a lock nobody would release.
grant_next(#lock{waiting = W} = L) ->
    case queue:out(W) of
        {empty, _} ->
            L#lock{holder = undefined, waiting = W};
        {{value, {Pid, _} = From}, Rest} ->
            case is_process_alive(Pid) of
                true ->
                    gen_server:reply(From, ok),
                    L#lock{holder = {Pid, monitor(process, Pid)}, waiting = Rest};
                false ->
                    grant_next(L#lock{waiting = Rest})
            end
    end.
