-module(test).
-export([
    run/0,
    node1_random/1,
    node2_perf/0,
    node3_failure/1,
    node4_replication/1,
    replication/2,
    bench/5,
    start/3,
    start_ids/2,
    add/2,
    check/2,
    stop/1
]).

-define(Settle, 1000).
-define(Timeout, 5000).
-define(Runs, 3).
-define(Space, 1000000000).

run() ->
    % node1_random(8),
    % node2_perf(),
    % node3_failure(6),
    node4_replication(6),
    ok.

node1_random(N) ->
    io:format("~n=== node1: ~w nodes, each joins via a random peer ===~n", [N]),
    Nodes = start(node1, N, random),

    io:format("waiting ~w ms for stabilization...~n", [?Settle * (N + 2)]),
    timer:sleep(?Settle * (N + 2)),
    Sorted = lists:keysort(1, Nodes),

    io:format("expected ring (sorted by key):~n"),
    [io:format("  ~10w  ~w~n", [K, P]) || {K, P} <- Sorted],
    [{_, First} | _] = Nodes,
    {_, Last} = lists:last(Nodes),

    io:format(
        "expected probe order from ~w:~n  ~w~n",
        [First, rotate(First, [P || {_, P} <- Sorted])]
    ),
    First ! probe,
    timer:sleep(500),

    io:format(
        "expected probe order from ~w:~n  ~w~n",
        [Last, rotate(Last, [P || {_, P} <- Sorted])]
    ),
    Last ! probe,
    timer:sleep(500),
    stop(Nodes).

node2_perf() ->
    io:format("~n=== node2: performance (ms, mean of ~w runs) ===~n", [?Runs]),
    io:format(
        "~-7s ~-5s ~-8s ~-8s ~-7s ~7s ~9s ~11s ~11s ~9s ~7s~n",
        [
            "layout",
            "ring",
            "clients",
            "per-cli",
            "access",
            "share",
            "add(max)",
            "lookup(avg)",
            "lookup(max)",
            "total",
            "failed"
        ]
    ),

    %% one node: four clients x 1000 vs one client x 4000
    bench(random, 1, 1, 4000, same),
    bench(random, 1, 4, 1000, same),

    %% add a second node, then two more, for each ring layout
    [
        bench(Layout, R, 4, 1000, Access)
     || Layout <- [random, even, uneven], R <- [2, 4], Access <- [same, spread]
    ],

    %% ten thousand elements
    bench(random, 1, 1, 10000, same),
    bench(random, 1, 4, 2500, same),
    [
        bench(Layout, 4, 4, 2500, Access)
     || Layout <- [random, even, uneven], Access <- [same, spread]
    ],
    ok.

node3_failure(N) ->
    io:format("~n=== node3: ~w nodes, one node is killed ===~n", [N]),
    Nodes = start(node3, N, random),
    timer:sleep(?Settle * (N + 2)),
    [{_, First} | Rest] = Nodes,

    Keys = add(1000, First),
    io:format("added ~w keys, found before crash: ~w~n",
              [length(Keys), length(Keys) - check(Keys, First)]),
    First ! probe,
    timer:sleep(500),

    {VKey, Victim} = lists:nth(rand:uniform(length(Rest)), Rest),
    io:format("killing node ~w (key ~w)~n", [Victim, VKey]),
    exit(Victim, kill),
    timer:sleep(?Settle * 3),

    First ! probe,
    timer:sleep(500),
    io:format("found after crash: ~w of ~w (the dead node's keys are lost)~n",
              [length(Keys) - check(Keys, First), length(Keys)]),
    stop(Nodes).

node4_replication(N) ->
    replication(node3, N),
    replication(node4, N).

replication(Module, N) ->
    io:format("~n=== ~w: ~w nodes, replication under joins and crashes ===~n",
              [Module, N]),
    Nodes = start(Module, N, random),
    timer:sleep(?Settle * (N + 2)),
    [{_, First} | _] = Nodes,

    Keys1 = add_acked(1000, First),
    io:format("added ~w keys (~w confirmed)~n", [1000, length(Keys1)]),

    Self = self(),
    spawn(fun() -> Self ! {keys, add_acked(1000, First)} end),
    JKey = key:generate(),
    {_, Peer} = lists:nth(rand:uniform(length(Nodes)), Nodes),
    Joined = Module:start(JKey, Peer),
    io:format("node ~w (key ~w) joins while adding~n", [Joined, JKey]),
    Keys2 = receive {keys, K} -> K end,
    io:format("added ~w keys during join (~w confirmed)~n", [1000, length(Keys2)]),
    timer:sleep(?Settle * 3),

    Keys = Keys1 ++ Keys2,
    Alive = Nodes ++ [{JKey, Joined}],
    report("after join", Keys, First),
    First ! probe,
    timer:sleep(500),

    Alive2 = kill_one(Alive),
    report("after 1st crash", Keys, First),
    Alive3 = kill_one(Alive2),
    report("after 2nd crash", Keys, First),
    First ! probe,
    timer:sleep(500),
    stop(Alive3).

%% Kill a random node other than the first one (the test's entry point).
kill_one([First | Rest]) ->
    {VKey, Victim} = lists:nth(rand:uniform(length(Rest)), Rest),
    io:format("killing node ~w (key ~w)~n", [Victim, VKey]),
    exit(Victim, kill),
    timer:sleep(?Settle * 3),
    [First | lists:keydelete(VKey, 1, Rest)].

report(When, Keys, Node) ->
    Found = length(Keys) - check(Keys, Node),
    io:format("~-16s found ~w of ~w~n", [When, Found, length(Keys)]).

%% Add N random keys through Node; return the keys that were confirmed.
add_acked(N, Node) ->
    Keys = [key:generate() || _ <- lists:seq(1, N)],
    [Key || Key <- Keys, add_one(Key, Node) == ok].

%% Run one configuration ?Runs times and print the mean of every column.
%% Layout = random | even | uneven decides the node ids (see ids/2),
%% Access = same | spread decides which node each client contacts.
bench(Layout, Ring, Clients, PerClient, Access) ->
    Runs = [bench_once(Layout, Ring, Clients, PerClient, Access) || _ <- lists:seq(1, ?Runs)],
    Mean = fun(I) -> lists:sum([element(I, R) || R <- Runs]) / ?Runs end,

    io:format(
        "~-7s ~-5w ~-8w ~-8w ~-7s ~5.1f% ~9.1f ~11.1f ~11.1f ~9.1f ~7w~n",
        [
            Layout,
            Ring,
            Clients,
            PerClient,
            Access,
            Mean(1),
            Mean(2) / 1000,
            Mean(3) / 1000,
            Mean(4) / 1000,
            Mean(5) / 1000,
            lists:sum([element(6, R) || R <- Runs])
        ]
    ).

%% Build a fresh ring, let Clients processes each add and then look up
%% PerClient random keys. Returns
%% {MaxShare%, AddMax, LookupAvg, LookupMax, Total, Failed} (times in us).
bench_once(Layout, Ring, Clients, PerClient, Access) ->
    Ids = ids(Layout, Ring),
    Nodes = start_ids(node2, Ids),
    timer:sleep(?Settle * (Ring + 2)),
    Pids = [P || {_, P} <- Nodes],
    Self = self(),

    T0 = erlang:monotonic_time(microsecond),
    [
        spawn(fun() -> client(Self, contact(Access, I, Pids), PerClient) end)
     || I <- lists:seq(1, Clients)
    ],

    Results = [
        receive
            {client, R} -> R
        end
     || _ <- lists:seq(1, Clients)
    ],

    Total = erlang:monotonic_time(microsecond) - T0,
    AddMax = lists:max([A || {A, _, _} <- Results]),
    Lookups = [L || {_, L, _} <- Results],
    Failed = lists:sum([F || {_, _, F} <- Results]),
    stop(Nodes),
    {max_share(Ids), AddMax, lists:sum(Lookups) / Clients, lists:max(Lookups), Total, Failed}.

%% Node ids for a ring of N nodes, in the order they are started; the
%% first id is the node used by `same` access.
%%   random: random ids
%%   even:   evenly spaced, every node owns 1/N of the key space
%%   uneven: N-1 nodes packed into (0, 200M], the last node at 1000M
%%           owns 80% of the key space; the first node is N-1 hops away
%%           from it
ids(_, 1) ->
    [key:generate()];
ids(random, N) ->
    [key:generate() || _ <- lists:seq(1, N)];
ids(even, N) ->
    [I * (?Space div N) || I <- lists:seq(1, N)];
ids(uneven, N) ->
    [I * (200000000 div (N - 1)) || I <- lists:seq(1, N - 1)] ++ [?Space].

%% Largest share of the key space (in %) owned by a single node.
%% A node owns (predecessor id, own id].
max_share([_]) ->
    100.0;
max_share(Ids) ->
    Sorted = lists:sort(Ids),
    Prevs = [lists:last(Sorted) | lists:droplast(Sorted)],
    lists:max([
        ((Id - Prev + ?Space) rem ?Space) * 100 / ?Space
     || {Id, Prev} <- lists:zip(Sorted, Prevs)
    ]).

contact(same, _, [First | _]) -> First;
contact(spread, I, Pids) -> lists:nth((I - 1) rem length(Pids) + 1, Pids).

%% One "test machine": add N keys, then look all of them up.
client(Parent, Node, N) ->
    {AddTime, Keys} = timer:tc(fun() -> add(N, Node) end),
    {LookTime, Failed} = timer:tc(fun() -> check(Keys, Node) end),
    Parent ! {client, {AddTime, LookTime, Failed}}.

%% Add N random key-value pairs through Node (pid or registered name).
%% Returns the list of keys.
add(N, Node) ->
    Keys = [key:generate() || _ <- lists:seq(1, N)],
    [add_one(Key, Node) || Key <- Keys],
    Keys.

add_one(Key, Node) ->
    Qref = make_ref(),
    Node ! {add, Key, value(Key), Qref, self()},

    receive
        {Qref, ok} -> ok
    after ?Timeout -> timeout
    end.

%% Look up all Keys through Node and return how many failed.
check(Keys, Node) ->
    length([Key || Key <- Keys, lookup_one(Key, Node) =/= {Key, value(Key)}]).

lookup_one(Key, Node) ->
    Qref = make_ref(),
    Node ! {lookup, Key, Qref, self()},

    receive
        {Qref, Result} -> Result
    after ?Timeout -> timeout
    end.

value(Key) -> {value, Key}.

%% Start N nodes of Module with random keys. Returns [{Key, Pid}].
start(Module, N, How) ->
    Key = key:generate(),
    First = Module:start(Key),
    log(Module, "started first node ~w (key ~w)~n", [First, Key]),
    start(Module, N - 1, How, [{Key, First}]).

start(_, 0, _, Acc) ->
    lists:reverse(Acc);
start(Module, N, How, Acc) ->
    Peer = peer(How, Acc),
    Key = key:generate(),
    Pid = Module:start(Key, Peer),
    log(Module, "started node ~w (key ~w), joined via ~w~n", [Pid, Key, Peer]),
    timer:sleep(100),
    start(Module, N - 1, How, [{Key, Pid} | Acc]).

%% Start nodes with the given ids, all joining via the first one.
start_ids(Module, [Id | Ids]) ->
    First = Module:start(Id),
    Rest = [
        begin
            Pid = Module:start(I, First),
            timer:sleep(100),
            {I, Pid}
        end
     || I <- Ids
    ],
    [{Id, First} | Rest].

%% Only node1 runs print every join; the perf runs stay quiet.
log(node1, Fmt, Args) -> io:format(Fmt, Args);
log(_, _, _) -> ok.

peer(first, Acc) ->
    {_, Pid} = lists:last(Acc),
    Pid;
peer(random, Acc) ->
    {_, Pid} = lists:nth(rand:uniform(length(Acc)), Acc),
    Pid.

%% Rotate list so it starts at Pid, like the probe list does.
rotate(Pid, List) ->
    {Before, After} = lists:splitwith(fun(P) -> P =/= Pid end, List),
    After ++ Before.

stop(Nodes) ->
    [exit(Pid, kill) || {_, Pid} <- Nodes],
    ok.
