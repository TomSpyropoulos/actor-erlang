%% @doc DB connection pool — one GenServer per connection (pool of 20).
%%
%% Writes are distributed across workers via round-robin. Queries are
%% fire-and-forget via epgsqla; results arrive in handle_info and are discarded.
-module(service_subscriber_db).
-behaviour(gen_server).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/1, init_counter/0, insert/5, insert_status/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(POOL_SIZE, 20).
-define(COUNTER_KEY, {?MODULE, round_robin_counter}).

-record(state, {
    db_pid             :: pid() | undefined,
    insert_stmt        :: #statement{} | undefined,
    insert_status_stmt :: #statement{} | undefined,
    pending            :: #{reference() => {integer(), integer()}}
}).

start_link(Index) ->
    Name = worker_name(Index),
    gen_server:start_link({local, Name}, ?MODULE, [Index], []).

%% Initialise the atomic round-robin counter. Must be called before any insert.
init_counter() ->
    Ref = atomics:new(1, [{signed, false}]),
    persistent_term:put(?COUNTER_KEY, Ref).

%% Atom name for pool slot Index, e.g. service_subscriber_db_3.
worker_name(Index) ->
    list_to_atom("service_subscriber_db_" ++ integer_to_list(Index)).

next_index() ->
    Ref = persistent_term:get(?COUNTER_KEY),
    N = atomics:add_get(Ref, 1, 1),
    ((N - 1) rem ?POOL_SIZE) + 1.

%% @doc Async insert of a sensor data row. Non-blocking for the caller.
%% MsgTimestampUs is the publisher timestamp in microseconds; ProcessingStartUs is
%% when the subscriber worker began handling the message. Both are carried through
%% so the DB worker can observe e2e and subscriber→DB write latencies on ack.
insert(DeviceName, Value, ErlTimestamp, MsgTimestampUs, ProcessingStartUs) ->
    gen_server:cast(worker_name(next_index()), {insert, DeviceName, Value, ErlTimestamp, MsgTimestampUs, ProcessingStartUs}).

%% @doc Async insert of a sensor status change row. Non-blocking for the caller.
insert_status(DeviceName, Status) ->
    gen_server:cast(worker_name(next_index()), {insert_status, DeviceName, Status}).

init([Index]) ->
    {ok, DB} = epgsql:connect("timescaledb", "postgres", "postgres", #{
        database => "epu",
        timeout  => 5000
    }),
    ?LOG_INFO("Connected to TimescaleDB worker ~p", [Index]),

    %% Parse statements once at startup to avoid per-query parse round-trips.
    {ok, InsertStmt} = epgsql:parse(DB, "insert_data_" ++ integer_to_list(Index),
                                    "INSERT INTO Data (DeviceName, Value, Timestamp) VALUES ($1, $2, $3)", []),
    {ok, InsertStatusStmt} = epgsql:parse(DB, "insert_status_" ++ integer_to_list(Index),
                                          "INSERT INTO sensor_status (DeviceName, Status) VALUES ($1, $2)", []),
    {ok, #state{
        db_pid             = DB,
        insert_stmt        = InsertStmt,
        insert_status_stmt = InsertStatusStmt,
        pending            = #{}
    }}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({insert, DeviceName, Value, ErlTimestamp, MsgTimestampUs, ProcessingStartUs},
            #state{db_pid = DB, insert_stmt = InsertStmt, pending = Pending} = State) ->
    #statement{types = Types} = InsertStmt,
    TypedParams = lists:zip(Types, [DeviceName, Value, ErlTimestamp]),
    Ref = epgsqla:prepared_query(DB, InsertStmt, TypedParams),
    {noreply, State#state{pending = Pending#{Ref => {MsgTimestampUs, ProcessingStartUs}}}};

handle_cast({insert_status, DeviceName, Status}, #state{db_pid = DB, insert_status_stmt = InsertStatusStmt} = State) ->
    #statement{types = Types} = InsertStatusStmt,
    TypedParams = lists:zip(Types, [DeviceName, Status]),
    epgsqla:prepared_query(DB, InsertStatusStmt, TypedParams),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Receives async query results from epgsqla. For data inserts, observe E2E latency.
handle_info({DB, Ref, _Result}, #state{db_pid = DB, pending = Pending} = State) when is_reference(Ref) ->
    NewPending = case maps:take(Ref, Pending) of
        {{MsgTimestampUs, ProcessingStartUs}, Rest} ->
            AckUs = os:system_time(microsecond),
            E2EUs     = max(0, AckUs - MsgTimestampUs),
            SubToDbUs = max(0, AckUs - ProcessingStartUs),
            service_subscriber_metrics:observe_e2e_latency(
                erlang:convert_time_unit(E2EUs, microsecond, native)),
            service_subscriber_metrics:observe_db_write_latency(
                erlang:convert_time_unit(SubToDbUs, microsecond, native)),
            Rest;
        error ->
            Pending
    end,
    {noreply, State#state{pending = NewPending}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db_pid = DB}) ->
    case is_pid(DB) of
        true  -> epgsql:close(DB);
        false -> ok
    end,
    ok.