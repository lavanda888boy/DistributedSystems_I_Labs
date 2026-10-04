-module(node2).
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
    node(Id, Predecessor, Successor, Store).

connect(Id, nil) ->
    {ok, {Id, self()}};
connect(_, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},

    receive
        {Qref, Skey, Spid} ->
            {ok, {Skey, Spid}}
    after ?Timeout ->
        io:format("Time out: no response~n", [])
    end.

node(Id, Predecessor, Successor, Store) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id, self()},
            node(Id, Predecessor, Successor, Store);
        {notify, New} ->
            {Pred, Keep} = notify(New, Id, Predecessor, Store),
            node(Id, Pred, Successor, Keep);
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor, Store);
        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor, Store);
        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Store);
        {request, Peer} ->
            request(Peer, Predecessor),
            node(Id, Predecessor, Successor, Store);
        {status, Pred} ->
            Succ = stabilize(Pred, Id, Successor),
            node(Id, Predecessor, Succ, Store);
        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Store);
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
            node(Id, Predecessor, Successor, Added);
        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Store);
        {handover, Elements} ->
            Merged = store:merge(Elements, Store),
            node(Id, Predecessor, Successor, Merged)
    end.

notify({Nkey, Npid}, Id, Predecessor, Store) ->
    case Predecessor of
        nil ->
            Keep = handover(Id, Store, Nkey, Npid),
            Npid ! {status, {Nkey, Npid}},
            {{Nkey, Npid}, Keep};
        {Pkey, _} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    Keep = handover(Id, Store, Nkey, Npid),
                    Npid ! {status, {Nkey, Npid}},
                    {{Nkey, Npid}, Keep};
                false ->
                    Npid ! {status, Predecessor},
                    {Predecessor, Store}
            end
    end.

handover(Id, Store, Nkey, Npid) ->
    {Rest, Keep} = store:split(Id, Nkey, Store),
    Npid ! {handover, Rest},
    Keep.

request(Peer, Predecessor) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil};
        {Pkey, Ppid} ->
            Peer ! {status, {Pkey, Ppid}}
    end.

stabilize({_, Spid}) ->
    Spid ! {request, self()}.

stabilize(Pred, Id, Successor) ->
    {Skey, Spid} = Successor,

    case Pred of
        nil ->
            Spid ! {notify, {Id, self()}},
            Successor;
        {Id, _} ->
            Successor;
        {Skey, _} ->
            Spid ! {notify, {Id, self()}},
            Successor;
        {Xkey, _} ->
            case key:between(Xkey, Id, Skey) of
                true ->
                    stabilize(Pred),
                    Pred;
                false ->
                    Spid ! {notify, {Id, self()}},
                    Successor
            end
    end.

add(Key, Value, Qref, Client, Id, {Pkey, _}, {_, Spid}, Store) ->
    case key:between(Key, Pkey, Id) of
        true ->
            Client ! {Qref, ok},
            store:add(Key, Value, Store);
        false ->
            Spid ! {add, Key, Value, Qref, Client},
            Store
    end.

lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, Store) ->
    case key:between(Key, Pkey, Id) of
        true ->
            Result = store:lookup(Key, Store),
            Client ! {Qref, Result};
        false ->
            {_, Spid} = Successor,
            Spid ! {lookup, Key, Qref, Client}
    end.

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

create_probe(Id, {_, Spid}) ->
    Spid ! {probe, Id, [self()], erlang:system_time(micro_seconds)}.

remove_probe(T, Nodes) ->
    Time = erlang:system_time(micro_seconds) - T,
    
    io:format(
        "Probe: ~w nodes in ring, ~w microseconds round trip~n  ~w~n",
        [length(Nodes), Time, Nodes]
    ).

forward_probe(Ref, T, Nodes, _Id, {_, Spid}) ->
    Spid ! {probe, Ref, Nodes ++ [self()], T}.
