-module(router).
-export([start/2, stop/1, status/1]).

start(Reg, Name) ->
    register(Reg, spawn(fun() -> init(Name) end)).

stop(Node) ->
    Node ! stop,
    unregister(Node).

init(Name) ->
    Intf = interface:new(),
    Map = map:new(),
    Table = dijkstra:table(Intf, Map),
    Hist = history:new(Name),
    router(Name, 0, Hist, Intf, Table, Map).

router(Name, N, Hist, Intf, Table, Map) ->
    receive
        {add, Node, Pid} ->
            Ref = erlang:monitor(process, Pid),
            Intf1 = interface:add(Node, Ref, Pid, Intf),
            router(Name, N, Hist, Intf1, Table, Map);
        {remove, Node} ->
            {ok, Ref} = interface:ref(Node, Intf),
            erlang:demonitor(Ref),
            Intf1 = interface:remove(Node, Intf),
            router(Name, N, Hist, Intf1, Table, Map);
        {'DOWN', Ref, process, _, _} ->
            {ok, Down} = interface:name(Ref, Intf),
            io:format("~w: exit received from ~w~n", [Name, Down]),
            Intf1 = interface:remove(Down, Intf),
            router(Name, N, Hist, Intf1, Table, Map);
        {status, From} ->
            From ! {status, {Name, N, Hist, Intf, Table, Map}},
            router(Name, N, Hist, Intf, Table, Map);
        {links, Node, R, Links} ->
            case history:update(Node, R, Hist) of
                {new, Hist1} ->
                    interface:broadcast({links, Node, N, Links}, Intf),
                    Map1 = map:update(Node, Links, Map),
                    router(Name, N, Hist1, Intf, Table, Map1);
                old ->
                    router(Name, N, Hist, Intf, Table, Map)
            end;
        update ->
            Table1 = dijkstra:table(interface:list(Intf), Map),
            router(Name, N, Hist, Intf, Table1, Map);
        broadcast ->
            Message = {links, Name, N, interface:list(Intf)},
            interface:broadcast(Message, Intf),
            router(Name, N + 1, Hist, Intf, Table, Map);
        {route, Name, _, Message} ->
            io:format("~w: received message ~w ~n", [Name, Message]),
            router(Name, N, Hist, Intf, Table, Map);
        {route, To, From, Message} ->
            io:format("~w: routing message (~w)~n", [Name, Message]),

            case dijkstra:route(To, Table) of
                {ok, Gateway} ->
                    case interface:lookup(Gateway, Intf) of
                        {ok, Pid} ->
                            Pid ! {route, To, From, Message};
                        notfound ->
                            ok
                    end;
                notfound ->
                    ok
            end,

            router(Name, N, Hist, Intf, Table, Map);
        {send, To, Message} ->
            self() ! {route, To, Name, Message},
            router(Name, N, Hist, Intf, Table, Map);
        stop ->
            ok
    end.

status(Node) ->
    Node ! {status, self()},

    receive
        {status, {Name, N, Hist, Intf, Table, Map}} ->
            io:format("Name: ~w~n", [Name]),
            io:format("Sequence number: ~w~n", [N]),
            io:format("History: ~w~n", [Hist]),
            io:format("Interfaces: ~w~n", [Intf]),
            io:format("Routing table: ~w~n", [Table]),
            io:format("Map: ~w~n~n", [Map])
    end.
