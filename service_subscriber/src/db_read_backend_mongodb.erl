%% @doc MongoDB backend implementing the db_read_backend behaviour. One connection per reader, opened
%% through db_backend_mongodb:connect/0 so a reader cannot be configured apart from the writers.
%%
%% The aggregate goes out as one raw OP_MSG command, whose reply carries the whole result in its first
%% batch. mc_worker_api:command/2 would start a cursor process per read for a single document.
%%
%% Reads the five DB_* connection variables through db_backend_mongodb; their defaults live in
%% docker-compose.mongodb.yaml.
-module(db_read_backend_mongodb).
-behaviour(db_read_backend).

-include_lib("kernel/include/logger.hrl").
-include_lib("mongodb/include/mongo_protocol.hrl").

-export([init/1, read/1, terminate/1]).

%% State of one reader's connection.
%% `conn`: The mc_worker process owned by this reader alone.
%% `command`: The aggregate command, built once from ?READ_PIPELINE.
-record(mor_state, {
    conn    :: pid(),
    command :: #op_msg_command{}
}).

%% The MongoDB spelling of the read group's query, as JSON text so the two arms can hold the same
%% bytes. $$NOW keeps the window server-side like NOW(6); finding Y covers how it prunes buckets.
%% Byte-identical to MongoReadTarget.scala; the matching rule is per backend.
-define(READ_PIPELINE,
        <<"[{\"$match\":{\"$expr\":{\"$gt\":[\"$Timestamp\",{\"$subtract\":[\"$$NOW\",5000]}]}}},"
          "{\"$group\":{\"_id\":null,\"avg\":{\"$avg\":\"$Value\"},\"count\":{\"$sum\":1}}}]">>).

%% Opens this reader's connection and builds the command it will reuse. json:decode/1 turns JSON null
%% into the atom bson-erlang encodes as BSON null.
init(Index) ->
    Conn = db_backend_mongodb:connect(),
    Command = #op_msg_command{command_doc = [{<<"aggregate">>, <<"Data">>},
                                             {<<"pipeline">>, json:decode(?READ_PIPELINE)},
                                             {<<"cursor">>, #{}}]},
    ?LOG_INFO("MongoDB read backend reader ~p connected", [Index]),
    {ok, #mor_state{conn = Conn, command = Command}}.

%% Runs the aggregate and discards the result. The driver raises on a failed command, so the raise is
%% reported as a failed read and the reader leaves it uncounted.
read(#mor_state{conn = Conn, command = Command} = State) ->
    try mc_connection_man:op_msg_raw_result(Conn, Command) of
        _Reply -> {ok, State}
    catch
        Class:Reason -> {error, {Class, Reason}, State}
    end.

%% Closes the connection when the reader shuts down.
terminate(#mor_state{conn = Conn}) ->
    mc_worker_api:disconnect(Conn),
    ok.
