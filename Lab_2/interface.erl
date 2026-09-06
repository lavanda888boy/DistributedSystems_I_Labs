-module(interface).
-export([new/0, add/4, remove/2, lookup/2, ref/2, name/2, list/1, broadcast/2]).

new() ->
    [].

add(Name, Ref, Pid, Intf) ->
    [{Name, Ref, Pid} | Intf].

remove(Name, Intf) ->
    lists:keydelete(Name, 1, Intf).

lookup(Name, Intf) ->
    case lists:keyfind(Name, 1, Intf) of
        false ->
            notfound;
        {Name, _, Pid} ->
            {ok, Pid}
    end.

ref(Name, Intf) ->
    case lists:keyfind(Name, 1, Intf) of
        false ->
            notfound;
        {Name, Ref, _} ->
            {ok, Ref}
    end.

name(Ref, Intf) ->
    case lists:keyfind(Ref, 2, Intf) of
        false ->
            notfound;
        {Name, Ref, _} ->
            {ok, Name}
    end.

list(Intf) ->
    lists:map(
        fun({Name, _, _}) -> Name end,
        Intf
    ).

broadcast(Message, Intf) ->
    lists:map(
        fun({_, _, Pid}) -> Pid ! Message end,
        Intf
    ).
