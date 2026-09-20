-module(worker).
-export([start/5, stop/1, peers/2]).

start(Name, Logger, Seed, Sleep, Jitter) ->
    spawn_link(fun() -> init(Name, Logger, Seed, Sleep, Jitter) end).

stop(Worker) ->
    Worker ! stop.

init(Name, Log, Seed, Sleep, Jitter) ->
    rand:seed(exsplus, {Seed, Seed, Seed}),
    Timer = time:zero(),

    receive
        {peers, Peers} ->
            loop(Name, Log, Peers, Timer, Sleep, Jitter);
        stop ->
            ok
    end.

peers(Wrk, Peers) ->
    Wrk ! {peers, Peers}.

loop(Name, Log, Peers, Timer, Sleep, Jitter) ->
    Wait = rand:uniform(Sleep),

    receive
        {msg, Time, Msg} ->
            Time1 = time:merge(Timer, Time),
            Time2 = time:inc("", Time1),
            Log ! {log, Name, Time2, {received, Msg}},
            loop(Name, Log, Peers, Time2, Sleep, Jitter);
        stop ->
            ok;
        Error ->
            Log ! {log, Name, time, {error, Error}}
    after Wait ->
        Selected = select(Peers),
        Message = {hello, rand:uniform(100)},
        Time1 = time:inc("", Timer),

        Selected ! {msg, Time1, Message},
        jitter(Jitter),

        Log ! {log, Name, Time1, {sending, Message}},
        loop(Name, Log, Peers, Time1, Sleep, Jitter)
    end.

select(Peers) ->
    lists:nth(rand:uniform(length(Peers)), Peers).

jitter(0) -> ok;
jitter(Jitter) -> timer:sleep(rand:uniform(Jitter)).
