-module(history).
-export([new/1, update/3]).

new(Name) ->
    [{Name, inf}].

update(Node, N, History) ->
    ExistingEntry = lists:keyfind(Node, 1, History),

    case ExistingEntry of
        false ->
            {new, [{Node, N} | History]};
        {Node, ExistingN} ->
            if
                N =< ExistingN ->
                    old;
                N > ExistingN ->
                    Updated = lists:keydelete(Node, 1, History),
                    {new, [{Node, N} | Updated]}
            end
    end.
