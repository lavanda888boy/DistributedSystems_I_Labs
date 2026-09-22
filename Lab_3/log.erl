-module(log).
-export([start/2, start/3, stop/1]).

start(Nodes, TimeMod) ->
    start(Nodes, TimeMod, none).

start(Nodes, TimeMod, StatsPid) ->
    spawn_link(fun() -> init(Nodes, TimeMod, StatsPid) end).

stop(Logger) ->
    Logger ! stop.

init(Nodes, TimeMod, StatsPid) ->
    Clock = TimeMod:clock(Nodes),
    Queue = [],
    loop(TimeMod, StatsPid, Clock, Queue).

loop(TimeMod, StatsPid, Clock, Queue) ->
    receive
        {log, From, Time, Msg} ->
            Clock1 = TimeMod:update(From, Time, Clock),
            Queue1 = [{From, Time, Msg} | Queue],
            Queue2 = process_message_queue(TimeMod, StatsPid, Clock1, Queue1),
            loop(TimeMod, StatsPid, Clock1, Queue2);
        stop ->
            ok
    end.

log(From, Time, Msg) ->
    io:format("log: ~w ~w ~p~n", [Time, From, Msg]).

process_message_queue(TimeMod, StatsPid, Clock, Queue) ->
    {Safe, Unsafe} =
        lists:partition(
            fun({_, Time, _}) ->
                TimeMod:safe(Time, Clock)
            end,
            Queue
        ),

    Ordered = lists:sort(
        fun({_, T1, _}, {_, T2, _}) -> TimeMod:leq(T1, T2) end,
        Safe
    ),

    lists:foreach(
        fun({From, Time, Msg}) ->
            log(From, Time, Msg)
        end,
        Ordered
    ),

    N = length(Unsafe),

    case N of
        0 -> ok;
        _ -> io:format("holdback: size ~w~n", [N])
    end,

    case StatsPid of
        none -> ok;
        _ -> StatsPid ! {holdback, N}
    end,

    Unsafe.
