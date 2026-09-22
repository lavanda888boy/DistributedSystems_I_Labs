-module(test).
-export([run/3, max_holdback/3, dynamic/3]).

run(Sleep, Jitter, TimeMod) ->
    Log = log:start([john, paul, ringo, george], TimeMod),

    A = worker:start(john, Log, TimeMod, 13, Sleep, Jitter),
    B = worker:start(paul, Log, TimeMod, 22, Sleep, Jitter),
    C = worker:start(ringo, Log, TimeMod, 36, Sleep, Jitter),
    D = worker:start(george, Log, TimeMod, 49, Sleep, Jitter),

    worker:peers(A, [B, C, D]),
    worker:peers(B, [A, C, D]),
    worker:peers(C, [A, B, D]),
    worker:peers(D, [A, B, C]),

    timer:sleep(5000),
    log:stop(Log),

    worker:stop(A),
    worker:stop(B),
    worker:stop(C),
    worker:stop(D).

max_holdback(Sleep, Jitter, time) ->
    max_holdback(Sleep, Jitter, time, [john, paul, ringo, george]);
max_holdback(Sleep, Jitter, vector) ->
    max_holdback(Sleep, Jitter, vector, []).

max_holdback(Sleep, Jitter, TimeMod, Nodes) ->
    flush_holdback(),
    Log = log:start(Nodes, TimeMod, self()),

    A = worker:start(john, Log, TimeMod, 13, Sleep, Jitter),
    B = worker:start(paul, Log, TimeMod, 22, Sleep, Jitter),
    C = worker:start(ringo, Log, TimeMod, 36, Sleep, Jitter),
    D = worker:start(george, Log, TimeMod, 49, Sleep, Jitter),

    worker:peers(A, [B, C, D]),
    worker:peers(B, [A, C, D]),
    worker:peers(C, [A, B, D]),
    worker:peers(D, [A, B, C]),

    Max = collect_max(0),

    log:stop(Log),
    worker:stop(A),
    worker:stop(B),
    worker:stop(C),
    worker:stop(D),

    io:format("~w: max holdback queue size = ~w~n", [TimeMod, Max]),
    Max.

collect_max(Max) ->
    receive
        {holdback, N} when N > Max -> collect_max(N)
    after 5000 ->
        Max
    end.

flush_holdback() ->
    receive
        {holdback, _} -> flush_holdback()
    after 0 ->
        ok
    end.

dynamic(Sleep, Jitter, TimeMod) ->
    Log = log:start([], TimeMod),

    A = worker:start(a, Log, TimeMod, 1, Sleep, Jitter),
    B = worker:start(b, Log, TimeMod, 2, Sleep, Jitter),
    C = worker:start(c, Log, TimeMod, 3, Sleep, Jitter),

    worker:peers(A, [B, C]),
    worker:peers(B, [A, C]),
    worker:peers(C, [A, B]),

    timer:sleep(2000),

    D = worker:start(d, Log, TimeMod, 5, Sleep, Jitter),
    E = worker:start(e, Log, TimeMod, 6, Sleep, Jitter),

    worker:peers(C, [A, B, D, E]),
    worker:peers(D, [C, E]),
    worker:peers(E, [C, D]),

    timer:sleep(2000),
    log:stop(Log),

    lists:foreach(fun worker:stop/1, [A, B, C, D, E]).
