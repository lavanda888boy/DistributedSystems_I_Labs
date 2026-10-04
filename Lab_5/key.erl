-module(key).
-export([generate/0, between/3]).

generate() ->
    Key = rand:uniform(1000000000),
    Key.

between(Key, From, To) when From < To ->
    if
        Key > From andalso Key =< To ->
            true;
        true ->
            false
    end;

between(Key, From, To) when From > To ->
    if
        Key > From orelse Key =< To ->
            true;
        true ->
            false
    end;

between(_, From, To) when From == To ->
    true.
