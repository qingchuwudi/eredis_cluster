-module(eredis_cluster_pool_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API.
-export([start_link/1]).
-export([query/2]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TIMEOUT, 1000).

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    Hostname = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    DataBase = proplists:get_value(database, Args, 0),
    Password = proplists:get_value(password, Args, ""),

    process_flag(trap_exit, true),
    Result = eredis:start_link(Hostname,Port, DataBase, Password),

    Conn = case Result of
        {ok,Connection} ->
            Connection;
        _ ->
            undefined
    end,

    {ok, #state{conn=Conn}}.

query(Worker, Commands) ->
    gen_server:call(Worker, {'query', Commands}).

handle_call({'query', _}, _From, #state{conn = undefined} = State) ->
    {reply, {error, no_connection}, State};

%% Timeout is necessary.
handle_call({'query', [[X|_]|_] = Commands}, _From, #state{conn = Conn} = State)
    when is_list(X); is_binary(X) ->
    case catch eredis:qp(Conn, Commands, ?TIMEOUT) of
        {'EXIT', Reason} ->
            {reply, {error, timeout}, State};
        Return ->
            {reply, Return, State}
    end;
handle_call({'query', Command}, _From, #state{conn = Conn} = State) ->
    case catch eredis:q(Conn, Command, ?TIMEOUT) of
        {'EXIT', Reason} ->
            {reply, {error, timeout}, State};
        Return ->
            {reply, Return, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% When a eredis_client loses connection with redis, the socket 
%% will be reset to 'undefined', and client will crash with any query.
%% 
handle_info({'EXIT', _Client, _Reason}, State) ->
    {stop, shutdown, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    ok = eredis:stop(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
