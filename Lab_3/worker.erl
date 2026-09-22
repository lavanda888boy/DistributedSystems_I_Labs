-module(worker).
-export([start/6, stop/1, peers/2]).

start(Name, Logger, TimeMod, Seed, Sleep, Jitter) ->
    spawn_link(fun() -> init(Name, Logger, TimeMod, Seed, Sleep, Jitter) end).

stop(Worker) ->
    Worker ! stop.

init(Name, Log, TimeMod, Seed, Sleep, Jitter) ->
    rand:seed(exsplus, {Seed, Seed, Seed}),
    Timer = TimeMod:zero(),

    receive
        {peers, Peers} ->
            loop(Name, Log, TimeMod, Peers, Timer, Sleep, Jitter);
        stop ->
            ok
    end.

peers(Wrk, Peers) ->
    Wrk ! {peers, Peers}.

loop(Name, Log, TimeMod, Peers, Timer, Sleep, Jitter) ->
    Wait = rand:uniform(Sleep),

    receive
        {msg, Time, Msg} ->
            Time1 = TimeMod:merge(Timer, Time),
            Time2 = TimeMod:inc(Name, Time1),
            Log ! {log, Name, Time2, {received, Msg}},
            loop(Name, Log, TimeMod, Peers, Time2, Sleep, Jitter);
        {peers, NewPeers} ->
            loop(Name, Log, TimeMod, NewPeers, Timer, Sleep, Jitter);
        stop ->
            ok;
        Error ->
            Log ! {log, Name, Timer, {error, Error}}
    after Wait ->
        Selected = select(Peers),
        Message = {hello, rand:uniform(100)},
        Time1 = TimeMod:inc(Name, Timer),

        Selected ! {msg, Time1, Message},
        jitter(Jitter),

        Log ! {log, Name, Time1, {sending, Message}},
        loop(Name, Log, TimeMod, Peers, Time1, Sleep, Jitter)
    end.

select(Peers) ->
    lists:nth(rand:uniform(length(Peers)), Peers).

jitter(0) -> ok;
jitter(Jitter) -> timer:sleep(rand:uniform(Jitter)).
