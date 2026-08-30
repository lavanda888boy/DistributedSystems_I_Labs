-module(test).
-export([bench/2, bench_parallel/3, bench_aggregate/3]).

bench(Host, Port) ->
    N = 100,
    Start = erlang:system_time(micro_seconds),

    run(N, Host, Port),

    Finish = erlang:system_time(micro_seconds),
    Time = Finish - Start,

    Seconds = Time / 1_000_000,
    RPS = N / Seconds,

    io:format(
        "Requests: ~p~nTime: ~p seconds~nRequests/sec: ~p~n",
        [N, Seconds, RPS]
    ),

    RPS.

bench_parallel(Clients, Host, Port) ->
    Parent = self(),

    Pids = [
        spawn(fun() ->
            Result = bench(Host, Port),
            Parent ! {done, self(), Result}
        end)
     || _ <- lists:seq(1, Clients)
    ],

    collect_results(Pids, []).

bench_aggregate(Clients, Host, Port) ->
    T0 = erlang:system_time(micro_seconds),

    bench_parallel(Clients, Host, Port),

    T1 = erlang:system_time(micro_seconds),
    TotalTime = (T1 - T0) / 1_000_000,
    TotalReqs = Clients * 100,
    RPS = TotalReqs / TotalTime,

    io:format(
        "~nAggregate (clients=~p)~nTotal requests: ~p~nWall time: ~p seconds~nAggregate requests/sec: ~p~n",
        [Clients, TotalReqs, TotalTime, RPS]
    ),

    RPS.

collect_results([], Results) ->
    Results;
collect_results(Pids, Results) ->
    receive
        {done, Pid, Result} ->
            collect_results(
                lists:delete(Pid, Pids),
                [Result | Results]
            )
    end.

run(N, Host, Port) ->
    if
        N == 0 ->
            ok;
        true ->
            request(Host, Port),
            run(N - 1, Host, Port)
    end.

request(Host, Port) ->
    Opt = [list, {active, false}, {reuseaddr, true}],
    {ok, Server} = gen_tcp:connect(Host, Port, Opt),

    gen_tcp:send(Server, http:get("foo")),
    Recv = gen_tcp:recv(Server, 0),

    case Recv of
        {ok, _} ->
            ok;
        {error, Error} ->
            io:format("test: error: ~w~n", [Error])
    end,

    gen_tcp:close(Server).
