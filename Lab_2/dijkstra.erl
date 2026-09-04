-module(dijkstra).
-export([table/2, route/2]).

entry(Node, Sorted) ->
    Element = lists:keyfind(Node, 1, Sorted),

    case Element of
        false ->
            0;
        {_, N, _} ->
            N
    end.

replace(Node, N, Gateway, Sorted) ->
    Element = lists:keyfind(Node, 1, Sorted),

    Sorted1 =
        case Element of
            false ->
                Sorted;
            {Node, _, _} ->
                lists:keydelete(Node, 1, Sorted)
        end,

    Sorted2 = [{Node, N, Gateway} | Sorted1],
    lists:sort(fun({_, N1, _}, {_, N2, _}) -> N1 =< N2 end, Sorted2).

update(Node, N, Gateway, Sorted) ->
    ExistingN = entry(Node, Sorted),

    if
        N < ExistingN ->
            replace(Node, N, Gateway, Sorted);
        N >= ExistingN ->
            Sorted
    end.

update_nodes([], _, _, Sorted) ->
    Sorted;
update_nodes([Node | Rest], N, Gateway, Sorted) ->
    Sorted1 = update(Node, N + 1, Gateway, Sorted),
    update_nodes(Rest, N, Gateway, Sorted1).

iterate([], _Map, Table) ->
    Table;
iterate([{_, inf, _} | _], _, Table) ->
    Table;
iterate([{Node, N, Gateway} | Rest], Map, Table) ->
    Neighbours = lists:keyfind(Node, 1, Map),

    Sorted1 =
        case Neighbours of
            false ->
                Rest;
            {Node, Nodes} ->
                update_nodes(Nodes, N, Node, Rest)
        end,

    Table1 = [{Node, Gateway} | Table],

    iterate(Sorted1, Map, Table1).

init_gateways([], Sorted) ->
    Sorted;
init_gateways([Gateway | Rest], Sorted) ->
    Sorted1 = replace(Gateway, 0, Gateway, Sorted),
    init_gateways(Rest, Sorted1).

table(Gateways, Map) ->
    Nodes = map:all_nodes(Map),
    Sorted = lists:map(fun(Node) -> {Node, inf, unknown} end, Nodes),
    Sorted1 = init_gateways(Gateways, Sorted),
    iterate(Sorted1, Map, []).

route(Node, Table) ->
    Element = lists:keyfind(Node, 1, Table),

    case Element of
        false ->
            notfound;
        {Node, Gateway} ->
            Gateway
    end.
