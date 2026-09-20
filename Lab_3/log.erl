-module(log).
-export([start/1, stop/1]).

start(Nodes) ->
    spawn_link(fun() -> init(Nodes) end).

stop(Logger) ->
    Logger ! stop.

init(Nodes) ->
    Clock = time:clock(Nodes),
    Queue = [],
    loop(Clock, Queue).

loop(Clock, Queue) ->
    receive
        {log, From, Time, Msg} ->
            Clock1 = time:update(From, Time, Clock),
            Queue1 = [{From, Time, Msg} | Queue],
            Queue2 = process_message_queue(Clock1, Queue1),
            loop(Clock1, Queue2);
        stop ->
            ok
    end.

log(From, Time, Msg) ->
    io:format("log: ~w ~w ~p~n", [Time, From, Msg]).

process_message_queue(Clock, Queue) ->
    {Safe, Unsafe} =
        lists:partition(
            fun({_, Time, _}) ->
                time:safe(Time, Clock)
            end,
            Queue
        ),

    lists:foreach(
        fun({From, Time, Msg}) ->
            log(From, Time, Msg)
        end,
        Safe
    ),

    Unsafe.
