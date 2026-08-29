-module(server_spawn).
-export([start/1, stop/0]).

start(Port) ->
    register(rudy, spawn(fun() -> init(Port) end)).

stop() ->
    exit(whereis(rudy), "time to die").

init(Port) ->
    Opt = [list, {active, false}, {reuseaddr, true}],

    case gen_tcp:listen(Port, Opt) of
        {ok, Listen} ->
            handler(Listen),
            gen_tcp:close(Listen),
            ok;
        {error, _} ->
            error
    end.

handler(Listen) ->
    case gen_tcp:accept(Listen) of
        {ok, Client} ->
            spawn(fun() -> request(Client) end),
            handler(Listen),
            ok;
        {error, _} ->
            error
    end.

request(Client) ->
    Recv = gen_tcp:recv(Client, 0),

    case Recv of
        {ok, Str} ->
            {Request, _, _} = http:parse_request(Str),
            Response = reply(Request),
            gen_tcp:send(Client, Response);
        {error, Error} ->
            io:format("server_spawn: error: ~w~n", [Error])
    end,

    gen_tcp:close(Client).

reply({get, URI, _}) ->
    timer:sleep(40),
    Resp = "Hello from Erlang to " ++ URI,
    http:ok(Resp).
