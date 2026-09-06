-module(test).
-export([run/0]).

routers() ->
    [{r1, stockholm}, {r2, gothenburg}, {r3, malmo}, {r4, uppsala}, {r5, lund}].

reg_names() ->
    lists:map(fun({Reg, _}) -> Reg end, routers()).

city_of(Reg) ->
    element(2, lists:keyfind(Reg, 1, routers())).

links() ->
    [{r1, r2}, {r2, r3}, {r3, r4}, {r4, r5}, {r5, r1}].

run() ->
    cleanup(),

    io:format("=== starting ~w routers ===~n", [length(routers())]),
    lists:foreach(fun({Reg, City}) -> router:start(Reg, City) end, routers()),

    io:format("=== wiring up links: ~w ===~n", [links()]),
    lists:foreach(fun({A, B}) -> connect(A, B) end, links()),

    io:format("=== broadcasting link-state (x2 to flood the ring) ===~n"),
    lists:foreach(fun(Reg) -> Reg ! broadcast end, reg_names()),
    timer:sleep(500),
    lists:foreach(fun(Reg) -> Reg ! broadcast end, reg_names()),
    timer:sleep(500),

    io:format("~n=== computing routing tables ===~n"),
    lists:foreach(fun(Reg) -> Reg ! update end, reg_names()),
    timer:sleep(500),

    lists:foreach(fun(Reg) -> router:status(Reg) end, reg_names()),

    io:format(
        "~n=== routing ~w -> ~w (expect 2 hops via ~w) ===~n",
        [city_of(r1), city_of(r3), city_of(r2)]
    ),
    r1 ! {send, city_of(r3), hello_from_stockholm},
    timer:sleep(500),

    io:format(
        "~n=== routing ~w -> ~w (expect 2 hops via ~w) ===~n",
        [city_of(r4), city_of(r1), city_of(r5)]
    ),
    r4 ! {send, city_of(r1), hello_from_uppsala},
    timer:sleep(500),

    io:format("~n=== simulating link failure: stopping ~w ===~n", [city_of(r2)]),
    router:stop(r2),
    timer:sleep(200),

    io:format("~n=== survivors re-broadcast (x2) and update after the failure ===~n"),
    Survivors = reg_names() -- [r2],
    lists:foreach(fun(Reg) -> Reg ! broadcast end, Survivors),
    timer:sleep(500),
    lists:foreach(fun(Reg) -> Reg ! broadcast end, Survivors),
    timer:sleep(500),
    lists:foreach(fun(Reg) -> Reg ! update end, Survivors),
    timer:sleep(500),

    io:format(
        "~n=== routing ~w -> ~w again (expect reroute via ~w, ~w) ===~n",
        [city_of(r1), city_of(r3), city_of(r5), city_of(r4)]
    ),
    r1 ! {send, city_of(r3), hello_after_failure},
    timer:sleep(500),

    io:format("~n=== stopping remaining routers ===~n"),
    lists:foreach(fun(Reg) -> router:stop(Reg) end, Survivors),

    io:format("=== test done ===~n"),
    ok.

cleanup() ->
    lists:foreach(
        fun(Reg) ->
            case whereis(Reg) of
                undefined ->
                    ok;
                _Pid ->
                    router:stop(Reg)
            end
        end,
        reg_names()
    ).

connect(RegA, RegB) ->
    PidA = whereis(RegA),
    PidB = whereis(RegB),
    RegA ! {add, city_of(RegB), PidB},
    RegB ! {add, city_of(RegA), PidA}.
