-module(map).
-export([new/0, update/3, reachable/2, all_nodes/1]).

new() ->
    [].

update(Node, Links, Map) ->
    Element = lists:keyfind(Node, 1, Map),

    Map1 =
        case Element of
            false ->
                Map;
            {Node, Links} ->
                lists:keydelete(Node, 1, Map)
        end,

    [{Node, Links} | Map1].

reachable(Node, Map) ->
    Element = lists:keyfind(Node, 1, Map),

    case Element of
        false ->
            [];
        {Node, Links} ->
            Links
    end.

all_nodes(Map) ->
    Sources = lists:map(fun({Node, _}) -> Node end, Map),
    Destinations = lists:flatmap(fun({_, Links}) -> Links end, Map),
    lists:uniq(Sources ++ Destinations).
