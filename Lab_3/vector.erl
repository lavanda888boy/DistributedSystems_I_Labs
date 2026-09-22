-module(vector).
-export([zero/0, inc/2, merge/2, leq/2, clock/1, update/3, safe/2]).

zero() ->
    [].

inc(Name, T) ->
    case lists:keyfind(Name, 1, T) of
        {Name, Counter} ->
            lists:keyreplace(Name, 1, T, {Name, Counter + 1});
        false ->
            [{Name, 1} | T]
    end.

merge([], Time) ->
    Time;
merge([{Name, Ti} | Rest], Time) ->
    case lists:keyfind(Name, 1, Time) of
        {Name, Tj} ->
            [{Name, max(Ti, Tj)} | merge(Rest, lists:keydelete(Name, 1, Time))];
        false ->
            [{Name, Ti} | merge(Rest, Time)]
    end.

leq([], _) ->
    true;
leq([{Name, Ti} | Rest], Time) ->
    case lists:keyfind(Name, 1, Time) of
        {Name, Tj} ->
            if
                Ti =< Tj ->
                    leq(Rest, Time);
                true ->
                    false
            end;
        false ->
            case Ti =< 0 of
                true ->
                    leq(Rest, Time);
                false ->
                    false
            end
    end.

clock(_) ->
    [].

update(From, Time, Clock) ->
    {_, Counter} = lists:keyfind(From, 1, Time),

    case lists:keyfind(From, 1, Clock) of
        {From, _} ->
            lists:keyreplace(From, 1, Clock, {From, Counter});
        false ->
            [{From, Counter} | Clock]
    end.

safe(Time, Clock) ->
    leq(Time, Clock).
