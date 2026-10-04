-module(node4).
-export([start/1, start/2]).

-define(Stabilize, 1000).
-define(Timeout, 10000).

start(Id) ->
    start(Id, nil).

start(Id, Peer) ->
    timer:start(),
    spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
    Predecessor = nil,
    {ok, Successor} = connect(Id, Peer),
    schedule_stabilize(),
    Store = store:create(),
    Replica = store:create(),
    node(Id, Predecessor, Successor, nil, Store, Replica).

connect(Id, nil) ->
    {ok, {Id, nil, self()}};
connect(_, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},

    receive
        {Qref, Skey, Spid} ->
            {ok, {Skey, monitor(Spid), Spid}}
    after ?Timeout ->
        io:format("Time out: no response~n", [])
    end.

node(Id, Predecessor, Successor, Next, Store, Replica) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id, self()},
            node(Id, Predecessor, Successor, Next, Store, Replica);
        {notify, New} ->
            {Pred, Keep, Rep} = notify(New, Id, Predecessor, Store, Replica),
            node(Id, Pred, Successor, Next, Keep, Rep);
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor, Next, Store, Replica);
        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor, Next, Store, Replica);
        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Next, Store, Replica);
        {request, Peer} ->
            request(Peer, Predecessor, Successor),
            node(Id, Predecessor, Successor, Next, Store, Replica);
        {status, Pred, Nx} ->
            {Succ, Nxt} = stabilize(Pred, Nx, Id, Successor),
            node(Id, Predecessor, Succ, Nxt, Store, Replica);
        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Next, Store, Replica);
        {add, Key, Value, Qref, Client} ->
            Added = add(
                Key,
                Value,
                Qref,
                Client,
                Id,
                Predecessor,
                Successor,
                Store
            ),
            node(Id, Predecessor, Successor, Next, Added, Replica);
        {replicate, Key, Value, Qref, Client, From} ->
            Rep = replicate(Key, Value, Qref, Client, From, Predecessor, Replica),
            node(Id, Predecessor, Successor, Next, Store, Rep);
        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Store, Replica);
        {handover, Elements, Rep} ->
            Merged = store:merge(Elements, Store),
            MergedRep = store:merge(Rep, Replica),
            node(Id, Predecessor, Successor, Next, Merged, MergedRep);
        {replica_request, Peer} ->
            Peer ! {replica, self(), Store},
            node(Id, Predecessor, Successor, Next, Store, Replica);
        {replica, From, Elements} ->
            Rep = replica(From, Elements, Predecessor, Replica),
            node(Id, Predecessor, Successor, Next, Store, Rep);
        {'DOWN', Ref, process, _, _} ->
            {Pred, Succ, Nxt, Keep, Rep} =
                down(Ref, Predecessor, Successor, Next, Store, Replica),
            node(Id, Pred, Succ, Nxt, Keep, Rep)
    end.

notify({Nkey, Npid}, Id, Predecessor, Store, Replica) ->
    case Predecessor of
        nil ->
            {Keep, Rest} = handover(Id, Store, Replica, Nkey, Npid),
            Nref = monitor(Npid),
            Npid ! {replica_request, self()},
            {{Nkey, Nref, Npid}, Keep, Rest};
        {Pkey, Pref, _} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    {Keep, Rest} = handover(Id, Store, Replica, Nkey, Npid),
                    drop(Pref),
                    Nref = monitor(Npid),
                    Npid ! {replica_request, self()},
                    {{Nkey, Nref, Npid}, Keep, Rest};
                false ->
                    {Predecessor, Store, Replica}
            end
    end.

handover(Id, Store, Replica, Nkey, Npid) ->
    {Rest, Keep} = store:split(Id, Nkey, Store),
    Npid ! {handover, Rest, Replica},
    {Keep, Rest}.

request(Peer, Predecessor, {Skey, _, Spid}) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil, {Skey, Spid}};
        {Pkey, _, Ppid} ->
            Peer ! {status, {Pkey, Ppid}, {Skey, Spid}}
    end.

stabilize({_, _, Spid}) ->
    Spid ! {request, self()}.

stabilize(Pred, Nx, Id, Successor) ->
    {Skey, Sref, Spid} = Successor,

    case Pred of
        nil ->
            Spid ! {notify, {Id, self()}},
            {Successor, Nx};
        {Id, _} ->
            {Successor, Nx};
        {Skey, _} ->
            Spid ! {notify, {Id, self()}},
            {Successor, Nx};
        {Xkey, Xpid} ->
            case key:between(Xkey, Id, Skey) of
                true ->
                    Xpid ! {request, self()},
                    drop(Sref),
                    Xref = monitor(Xpid),
                    {{Xkey, Xref, Xpid}, {Skey, Spid}};
                false ->
                    Spid ! {notify, {Id, self()}},
                    {Successor, Nx}
            end
    end.

down(Ref, {_, Ref, _}, Successor, Next, Store, Replica) ->
    Merged = store:merge(Replica, Store),
    {_, _, Spid} = Successor,
    Spid ! {replica, self(), Merged},
    {nil, Successor, Next, Merged, store:create()};
down(Ref, Predecessor, {_, Ref, _}, {Nkey, Npid}, Store, Replica) ->
    Nref = monitor(Npid),
    Npid ! {request, self()},
    {Predecessor, {Nkey, Nref, Npid}, nil, Store, Replica}.

monitor(Pid) ->
    erlang:monitor(process, Pid).

drop(nil) ->
    ok;
drop(Ref) ->
    erlang:demonitor(Ref, [flush]).

replica(From, Elements, {_, _, From}, _) ->
    Elements;
replica(_, _, _, Replica) ->
    Replica.

add(Key, Value, Qref, Client, _Id, nil, {_, _, Spid}, Store) ->
    Spid ! {replicate, Key, Value, Qref, Client, self()},
    store:add(Key, Value, Store);
add(Key, Value, Qref, Client, Id, {Pkey, _, _}, {_, _, Spid}, Store) ->
    case key:between(Key, Pkey, Id) of
        true ->
            Spid ! {replicate, Key, Value, Qref, Client, self()},
            store:add(Key, Value, Store);
        false ->
            Spid ! {add, Key, Value, Qref, Client},
            Store
    end.

replicate(Key, Value, Qref, Client, forwarded, _, Replica) ->
    Client ! {Qref, ok},
    store:add(Key, Value, Replica);
replicate(Key, Value, Qref, Client, From, {_, _, Ppid}, Replica) when Ppid =/= From ->
    Ppid ! {replicate, Key, Value, Qref, Client, forwarded},
    Replica;
replicate(Key, Value, Qref, Client, _, _, Replica) ->
    Client ! {Qref, ok},
    store:add(Key, Value, Replica).

lookup(Key, Qref, Client, _Id, nil, _Successor, Store) ->
    Client ! {Qref, store:lookup(Key, Store)};
lookup(Key, Qref, Client, Id, {Pkey, _, _}, Successor, Store) ->
    case key:between(Key, Pkey, Id) of
        true ->
            Result = store:lookup(Key, Store),
            Client ! {Qref, Result};
        false ->
            {_, _, Spid} = Successor,
            Spid ! {lookup, Key, Qref, Client}
    end.

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

create_probe(Id, {_, _, Spid}) ->
    Spid ! {probe, Id, [self()], erlang:system_time(micro_seconds)}.

remove_probe(T, Nodes) ->
    Time = erlang:system_time(micro_seconds) - T,

    io:format(
        "Probe: ~w nodes in ring, ~w microseconds round trip~n  ~w~n",
        [length(Nodes), Time, Nodes]
    ).

forward_probe(Ref, T, Nodes, _Id, {_, _, Spid}) ->
    Spid ! {probe, Ref, Nodes ++ [self()], T}.
