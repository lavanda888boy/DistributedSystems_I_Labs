-module(server_pool).
-export([start/2, stop/0]).

start(Port, NumAcceptors) ->
    ManagerPid = spawn(fun() -> manager_init(Port, NumAcceptors) end),
    register(rudy, ManagerPid),
    ok.

stop() ->
    exit(whereis(rudy), "time to die").

manager_init(Port, NumAcceptors) ->
    process_flag(trap_exit, false),
    Opt = [list, {active, false}, {reuseaddr, true}],

    case gen_tcp:listen(Port, Opt) of
        {ok, Listen} ->
            [
                spawn_link(fun() -> acceptor(Listen) end)
             || _ <- lists:seq(1, NumAcceptors)
            ],
            receive
            after infinity -> ok
            end;
        {error, Reason} ->
            {error, Reason}
    end.

acceptor(Listen) ->
    case gen_tcp:accept(Listen) of
        {ok, Client} ->
            request(Client),
            acceptor(Listen);
        {error, _} ->
            ok
    end.

request(Client) ->
    Recv = gen_tcp:recv(Client, 0),

    case Recv of
        {ok, Str} ->
            {Request, _, _} = http:parse_request(Str),
            Response = reply(Request),
            gen_tcp:send(Client, Response);
        {error, Error} ->
            io:format("server_pool: error: ~w~n", [Error])
    end,

    gen_tcp:close(Client).

reply({get, URI, _}) ->
    timer:sleep(40),
    Resp = "Hello from Erlang to " ++ URI,
    http:ok(Resp).
