-module(time).
-export([zero/0, inc/2, merge/2, leq/2, clock/1, update/3, safe/2]).

zero() ->
    0.

inc(_, T) ->
    T1 = T + 1,
    T1.

merge(Ti, Tj) ->
    Merged = max(Ti, Tj),
    Merged.

leq(Ti, Tj) ->
    if
        Ti =< Tj ->
            true;
        true ->
            false
    end.

clock(Nodes) ->
    Clock = lists:map(fun(Node) -> {Node, 0} end, Nodes),
    Clock.

update(Node, Time, Clock) ->
    Clock1 = lists:keyreplace(Node, 1, Clock, {Node, Time}),
    Clock1.

safe(Time, Clock) ->
    Safe = lists:all(fun({_, T}) -> leq(Time, T) end, Clock),
    Safe.
