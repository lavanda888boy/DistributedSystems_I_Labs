-module(node1).
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
    node(Id, Predecessor, Successor).

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

node(Id, Predecessor, Successor) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id, self()},
            node(Id, Predecessor, Successor);
        {notify, New} ->
            Pred = notify(New, Id, Predecessor),
            node(Id, Pred, Successor);
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor);
        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor);
        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor);
        {request, Peer} ->
            request(Peer, Predecessor),
            node(Id, Predecessor, Successor);
        {status, Pred} ->
            Succ = stabilize(Pred, Id, Successor),
            node(Id, Predecessor, Succ);
        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor)
    end.

notify({Nkey, Npid}, Id, Predecessor) ->
    case Predecessor of
        nil ->
            Npid ! {status, {Nkey, Npid}},
            {Nkey, Npid};
        {Pkey, _} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    Npid ! {status, {Nkey, Npid}},
                    {Nkey, Npid};
                false ->
                    Npid ! {status, Predecessor},
                    Predecessor
            end
    end.

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
