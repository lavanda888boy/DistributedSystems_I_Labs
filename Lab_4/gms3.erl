-module(gms3).
-export([start/1, start/2, init/3, init/4]).

-define(timeout, 1000).
-define(arghh, 200).
-define(ack_timeout, 300).
-define(max_retries, 5).
-define(drop_chance, 100).

leader(Id, Master, N, Slaves, Group, Seen, Pending) ->
    receive
        {mcast, Msg} ->
            Pending2 = bcast(Id, {msg, N, Msg}, Slaves, Pending),
            Master ! Msg,
            leader(Id, Master, N + 1, Slaves, Group, Seen, Pending2);
        {rmsg, Ref, Sender, {mcast, Msg}} ->
            Sender ! {ack, Ref},

            case is_seen(Ref, Seen) of
                true ->
                    leader(Id, Master, N, Slaves, Group, Seen, Pending);
                false ->
                    Pending2 = bcast(Id, {msg, N, Msg}, Slaves, Pending),
                    Master ! Msg,
                    leader(Id, Master, N + 1, Slaves, Group, remember(Ref, Seen), Pending2)
            end;
        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            Pending2 = bcast(Id, {view, N, [self() | Slaves2], Group2}, Slaves2, Pending),
            Master ! {view, Group2},
            leader(Id, Master, N + 1, Slaves2, Group2, Seen, Pending2);
        {ack, Ref} ->
            leader(Id, Master, N, Slaves, Group, Seen, maps:remove(Ref, Pending));
        {retry, Ref} ->
            Pending2 = handle_retry(Ref, Pending),
            leader(Id, Master, N, Slaves, Group, Seen, Pending2);
        stop ->
            ok
    end.

slave(Id, Master, Leader, N, Last, Slaves, Group, Seen, Pending, Buffer) ->
    receive
        {mcast, Msg} ->
            Pending2 = send_reliable(Leader, {mcast, Msg}, Pending),
            slave(Id, Master, Leader, N, Last, Slaves, Group, Seen, Pending2, Buffer);
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, N, Last, Slaves, Group, Seen, Pending, Buffer);
        {rmsg, Ref, Sender, Payload} ->
            Sender ! {ack, Ref},

            case is_seen(Ref, Seen) of
                true ->
                    slave(Id, Master, Leader, N, Last, Slaves, Group, Seen, Pending, Buffer);
                false ->
                    Seen2 = remember(Ref, Seen),

                    case Payload of
                        {msg, N, Msg} ->
                            Master ! Msg,
                            deliver_next(
                                Id, Master, Leader, N + 1, Payload, Slaves, Group, Seen2, Pending, Buffer
                            );
                        {msg, I, _} when I < N ->
                            slave(Id, Master, Leader, N, Last, Slaves, Group, Seen2, Pending, Buffer);
                        {msg, I, _} when I > N ->
                            slave(
                                Id, Master, Leader, N, Last, Slaves, Group, Seen2, Pending,
                                Buffer#{I => Payload}
                            );
                        {view, N, [Leader | Slaves2], Group2} ->
                            Master ! {view, Group2},
                            deliver_next(
                                Id, Master, Leader, N + 1, Payload, Slaves2, Group2, Seen2, Pending, Buffer
                            );
                        {view, I, _, _} when I < N ->
                            slave(Id, Master, Leader, N, Last, Slaves, Group, Seen2, Pending, Buffer);
                        {view, I, _, _} when I > N ->
                            slave(
                                Id, Master, Leader, N, Last, Slaves, Group, Seen2, Pending,
                                Buffer#{I => Payload}
                            )
                    end
            end;
        {ack, Ref} ->
            slave(
                Id, Master, Leader, N, Last, Slaves, Group, Seen, maps:remove(Ref, Pending), Buffer
            );
        {retry, Ref} ->
            Pending2 = handle_retry(Ref, Pending),
            slave(Id, Master, Leader, N, Last, Slaves, Group, Seen, Pending2, Buffer);
        {'DOWN', _Ref, process, Leader, _Reason} ->
            election(Id, Master, N, Last, Slaves, Group, Seen, Pending);
        stop ->
            ok
    after ?timeout ->
        Master ! {error, "no reply from leader"}
    end.

deliver_next(Id, Master, Leader, N, Last, Slaves, Group, Seen, Pending, Buffer) ->
    case maps:take(N, Buffer) of
        {{msg, N, Msg} = Payload, Buffer2} ->
            Master ! Msg,
            deliver_next(Id, Master, Leader, N + 1, Payload, Slaves, Group, Seen, Pending, Buffer2);
        {{view, N, [Leader | Slaves2], Group2} = Payload, Buffer2} ->
            Master ! {view, Group2},
            deliver_next(
                Id, Master, Leader, N + 1, Payload, Slaves2, Group2, Seen, Pending, Buffer2
            );
        {_StalePayload, Buffer2} ->
            slave(Id, Master, Leader, N, Last, Slaves, Group, Seen, Pending, Buffer2);
        error ->
            slave(Id, Master, Leader, N, Last, Slaves, Group, Seen, Pending, Buffer)
    end.

start(Id) ->
    Rnd = rand:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Rnd, Self) end)}.

init(Id, Rnd, Master) ->
    rand:seed(exsplus, {Rnd, Rnd, Rnd}),
    leader(Id, Master, 0, [], [Master], [], #{}).

start(Id, Grp) ->
    Rnd = rand:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Grp, Rnd, Self) end)}.

send_reliable(Node, Payload, Pending) ->
    Ref = make_ref(),
    dispatch(Node, Ref, Payload),
    erlang:send_after(?ack_timeout, self(), {retry, Ref}),
    Pending#{Ref => {Node, Payload, ?max_retries}}.

handle_retry(Ref, Pending) ->
    case maps:find(Ref, Pending) of
        {ok, {Node, _Payload, 0}} ->
            io:format("giving up on ~w, presumed unreachable~n", [Node]),
            maps:remove(Ref, Pending);
        {ok, {Node, Payload, Retries}} ->
            dispatch(Node, Ref, Payload),
            erlang:send_after(?ack_timeout, self(), {retry, Ref}),
            Pending#{Ref => {Node, Payload, Retries - 1}};
        error ->
            Pending
    end.

dispatch(Node, Ref, Payload) ->
    case rand:uniform(?drop_chance) of
        ?drop_chance ->
            io:format("~w: simulated drop of msg to ~w~n", [self(), Node]);
        _ ->
            Node ! {rmsg, Ref, self(), Payload}
    end.

is_seen(Ref, Seen) ->
    lists:member(Ref, Seen).

remember(Ref, Seen) ->
    lists:sublist([Ref | Seen], 50).

bcast(Id, Msg, Nodes, Pending) ->
    lists:foldl(
        fun(Node, PendingAcc) ->
            PendingAcc2 = send_reliable(Node, Msg, PendingAcc),
            crash(Id),
            PendingAcc2
        end,
        Pending,
        Nodes
    ).

crash(Id) ->
    case rand:uniform(?arghh) of
        ?arghh ->
            io:format("leader ~w: crash~n", [Id]),
            exit(no_luck);
        _ ->
            ok
    end.

init(Id, Grp, Rnd, Master) ->
    rand:seed(exsplus, {Rnd, Rnd, Rnd}),
    Self = self(),
    Grp ! {join, Master, Self},

    receive
        {rmsg, Ref, Sender, {view, N, [Leader | Slaves], Group}} ->
            Sender ! {ack, Ref},
            erlang:monitor(process, Leader),
            Master ! {view, Group},
            slave(
                Id,
                Master,
                Leader,
                N + 1,
                {view, N, [Leader | Slaves], Group},
                Slaves,
                Group,
                [Ref],
                #{},
                #{}
            )
    end.

election(Id, Master, N, Last, Slaves, [_ | Group], Seen, Pending) ->
    Self = self(),

    case Slaves of
        [Self | Rest] ->
            Pending2 = bcast(Id, Last, Rest, Pending),
            Master ! {view, Group},
            leader(Id, Master, N, Rest, Group, Seen, Pending2);
        [Leader | Rest] ->
            erlang:monitor(process, Leader),
            slave(Id, Master, Leader, N, Last, Rest, Group, Seen, Pending, #{})
    end.
