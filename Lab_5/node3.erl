-module(node3).
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
    node(Id, Predecessor, Successor, nil, Store).

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

node(Id, Predecessor, Successor, Next, Store) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id, self()},
            node(Id, Predecessor, Successor, Next, Store);
        {notify, New} ->
            {Pred, Keep} = notify(New, Id, Predecessor, Store),
            node(Id, Pred, Successor, Next, Keep);
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor, Next, Store);
        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor, Next, Store);
        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Next, Store);
        {request, Peer} ->
            request(Peer, Predecessor, Successor),
            node(Id, Predecessor, Successor, Next, Store);
        {status, Pred, Nx} ->
            {Succ, Nxt} = stabilize(Pred, Nx, Id, Successor),
            node(Id, Predecessor, Succ, Nxt, Store);
        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Next, Store);
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
            node(Id, Predecessor, Successor, Next, Added);
        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Store);
        {handover, Elements} ->
            Merged = store:merge(Elements, Store),
            node(Id, Predecessor, Successor, Next, Merged);
        {'DOWN', Ref, process, _, _} ->
            {Pred, Succ, Nxt} = down(Ref, Predecessor, Successor, Next),
            node(Id, Pred, Succ, Nxt, Store)
    end.

notify({Nkey, Npid}, Id, Predecessor, Store) ->
    case Predecessor of
        nil ->
            Keep = handover(Id, Store, Nkey, Npid),
            Nref = monitor(Npid),
            {{Nkey, Nref, Npid}, Keep};
        {Pkey, Pref, _} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    Keep = handover(Id, Store, Nkey, Npid),
                    drop(Pref),
                    Nref = monitor(Npid),
                    {{Nkey, Nref, Npid}, Keep};
                false ->
                    {Predecessor, Store}
            end
    end.

handover(Id, Store, Nkey, Npid) ->
    {Rest, Keep} = store:split(Id, Nkey, Store),
    Npid ! {handover, Rest},
    Keep.

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

down(Ref, {_, Ref, _}, Successor, Next) ->
    {nil, Successor, Next};
down(Ref, Predecessor, {_, Ref, _}, {Nkey, Npid}) ->
    Nref = monitor(Npid),
    Npid ! {request, self()},
    {Predecessor, {Nkey, Nref, Npid}, nil}.

monitor(Pid) ->
    erlang:monitor(process, Pid).

drop(nil) ->
    ok;
drop(Ref) ->
    erlang:demonitor(Ref, [flush]).

add(Key, Value, Qref, Client, Id, {Pkey, _, _}, {_, _, Spid}, Store) ->
    case key:between(Key, Pkey, Id) of
        true ->
            Client ! {Qref, ok},
            store:add(Key, Value, Store);
        false ->
            Spid ! {add, Key, Value, Qref, Client},
            Store
    end.

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
