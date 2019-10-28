-module(eredis_cluster_monitor).
-behaviour(gen_server).

%% API.
-export([start_link/0]).
-export([connect/2]).
-export([refresh_mapping/2]).
-export([get_state/1, get_state_version/1]).
-export([get_pool_by_slot/2, get_pool_by_slot/3]).
-export([get_all_pools/1]).
-export([get_all_clusters/0]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% Type definition.
-include("eredis_cluster.hrl").

-export_type([cluster_name/0]).

-type cluster_name() :: atom().

-record(cluster, {
    cluster_name :: cluster_name(),
    init_nodes :: [#node{}],
    slots_maps :: list(), %% whose elements are #slots_map{}
    version :: integer()
}).


%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

connect(ClusterName, InitServers) ->
    gen_server:call(?MODULE,{connect,ClusterName,InitServers}).

refresh_mapping(ClusterName, Version) ->
    gen_server:call(?MODULE,{reload_slots_map,ClusterName,Version}).

%% =============================================================================
%% @doc Given a slot return the link (Redis instance) to the mapped
%% node.
%% @end
%% =============================================================================

-spec get_state(cluster_name()) -> #cluster{}.
get_state(ClusterName) ->
    case ets:lookup(?MODULE, ClusterName) of
        [#cluster{} = Cluster] ->
            {ok, Cluster};
        [] ->
            undefined
    end.

get_state_version(Cluster) ->
    Cluster#cluster.version.

-spec get_all_pools(cluster_name()) -> [pid()].
get_all_pools(ClusterName) ->
    {ok, Cluster} = get_state(ClusterName),
    [SlotsMap#slots_map.node#node.pool
        || SlotsMap <- Cluster#cluster.slots_maps,
           SlotsMap#slots_map.node =/= undefined].

get_all_clusters() ->
    lists:foldl(
        fun(#cluster{cluster_name = ClusterName}, Acc) ->
            [ClusterName | Acc]
        end, [], ets:tab2list(?MODULE)).

%% =============================================================================
%% @doc Get cluster pool by slot. Optionally, a memoized Cluster can be provided
%% to prevent from querying ets inside loops.
%% @end
%% =============================================================================
-spec get_pool_by_slot(ClusterName :: cluster_name(), Slot::integer(), Cluster::#cluster{}) ->
    {PoolName::atom() | undefined, Version::integer()}.
get_pool_by_slot(_ClusterName, Slot, Cluster) ->
    SlotsMap = select_slot(Cluster#cluster.slots_maps, Slot),
    if
        SlotsMap#slots_map.node =/= undefined ->
            {SlotsMap#slots_map.node#node.pool, Cluster#cluster.version};
        true ->
            {undefined, Cluster#cluster.version}
    end.

-spec select_slot([#slots_map{}, ...], non_neg_integer()) -> #slots_map{}.
select_slot([#slots_map{start_slot = S,
                        end_slot = E} = SlotMap | _],
            Slot) when (S =< Slot) andalso (Slot =< E) ->
    SlotMap;
select_slot([_| Rest], Slot) ->
    select_slot(Rest, Slot).

-spec get_pool_by_slot(ClusterName :: cluster_name(), Slot::integer()) ->
    {PoolName::atom() | undefined, Version::integer()}.
get_pool_by_slot(ClusterName, Slot) ->
    {ok, Cluster} = get_state(ClusterName),
    get_pool_by_slot(ClusterName, Slot, Cluster).

-spec reload_slots_map(Cluster::#cluster{}) -> NewCluster::#cluster{}.
reload_slots_map(Cluster) ->
    [close_connection(SlotsMap) || SlotsMap <- Cluster#cluster.slots_maps],

    ClusterSlots = get_cluster_slots(Cluster#cluster.init_nodes),

    SlotsMaps = parse_cluster_slots(ClusterSlots, Cluster#cluster.init_nodes),
    ConnectedSlotsMaps = connect_all_slots(SlotsMaps),

    NewCluster = Cluster#cluster{
        slots_maps = ConnectedSlotsMaps,
        version = Cluster#cluster.version + 1
    },

    % Key = {NewCluster#cluster.cluster_name, cluster_state},

    true = ets:insert(?MODULE, NewCluster),

    NewCluster.

-spec get_cluster_slots([#node{}]) -> [[bitstring() | [bitstring()]]].
get_cluster_slots([]) ->
    throw({error,cannot_connect_to_cluster});
get_cluster_slots([Node|T]) ->
    case safe_eredis_start_link(Node) of
        {ok,Connection} ->
          case eredis:q(Connection, ["CLUSTER", "SLOTS"]) of
            % {error,<<"ERR unknown command 'CLUSTER'">>} ->
            %     get_cluster_slots_from_single_node(Node);
            % {error,<<"ERR This instance has cluster support disabled">>} ->
            %     get_cluster_slots_from_single_node(Node);
            {ok, ClusterInfo} ->
                eredis:stop(Connection),
                ClusterInfo;
            {error, Reason} ->
                throw({error, Reason});
            _ ->
                eredis:stop(Connection),
                get_cluster_slots(T)
        end;
        _ ->
            get_cluster_slots(T)
  end.

safe_eredis_start_link(#node{
        address = Address,
        port = Port,
        password = Password,
        reconnect_sleep = ReconnectSleep}) ->
    process_flag(trap_exit, true),
    Payload = eredis:start_link(Address, Port, 0, Password, ReconnectSleep),
    process_flag(trap_exit, false),
    Payload.

% -spec get_cluster_slots_from_single_node(#node{}) ->
%     [[bitstring() | [bitstring()]]].
% get_cluster_slots_from_single_node(Node) ->
%     [[<<"0">>, integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
%     [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]]].

-spec parse_cluster_slots([[bitstring() | [bitstring()]]], [#node{}, ...]) -> [#slots_map{}].
parse_cluster_slots(ClusterInfo, Nodes) ->
    parse_cluster_slots(ClusterInfo, Nodes, 1, []).

parse_cluster_slots([[StartSlot, EndSlot | [[Address, Port | _] | _]] | T], Nodes, Index, Acc) ->
    case lists:keyfind(Address, #node.address, Nodes) of
        false ->
            [Node|_] = Nodes;
        #node{} = Node ->
            Node
    end,
    SlotsMap =
        #slots_map{
            index = Index,
            start_slot = binary_to_integer(StartSlot),
            end_slot = binary_to_integer(EndSlot),
            node = Node#node{
                address = binary_to_list(Address),
                port = binary_to_integer(Port)
            }
        },
    parse_cluster_slots(T, Nodes, Index+1, [SlotsMap | Acc]);
parse_cluster_slots([], _Nodes, _Index, Acc) ->
    lists:reverse(Acc).



-spec close_connection(#slots_map{}) -> ok.
close_connection(SlotsMap) ->
    Node = SlotsMap#slots_map.node,
    if
        Node =/= undefined ->
            try eredis_cluster_pool:stop(Node#node.pool) of
                _ ->
                    ok
            catch
                _ ->
                    ok
            end;
        true ->
            ok
    end.

-spec connect_node(#node{}) -> #node{} | undefined.
connect_node(Node) ->
    case eredis_cluster_pool:create(Node) of
        {ok, Pool} ->
            Node#node{pool=Pool};
        _ ->
            undefined
    end.

% -spec create_slots_cache([#slots_map{}]) -> [integer()].
% create_slots_cache(SlotsMaps) ->
%   SlotsCache = [
%         [{Index,SlotsMap#slots_map.index} || Index <- lists:seq(SlotsMap#slots_map.start_slot, SlotsMap#slots_map.end_slot)]
%     || SlotsMap <- SlotsMaps],
%   SlotsCacheF = lists:flatten(SlotsCache),
%   SortedSlotsCache = lists:sort(SlotsCacheF),
%   [ Index || {_,Index} <- SortedSlotsCache].

-spec connect_all_slots([#slots_map{}]) -> [integer()].
connect_all_slots(SlotsMapList) ->
    [SlotsMap#slots_map{node=connect_node(SlotsMap#slots_map.node)}
        || SlotsMap <- SlotsMapList].

-spec connect_(ClusterName :: cluster_name(), [{Address::string(), Port::integer()}]) -> #cluster{}.
connect_(_ClusterName, []) ->
    #cluster{};
connect_(ClusterName, InitNodes) ->
    Cluster = #cluster{
        cluster_name = ClusterName,
        slots_maps = [],
        init_nodes = init_nodes(InitNodes),
        version = 0
    },
    reload_slots_map(Cluster).

%%@private 格式化配置参数
init_nodes([]) -> [];
init_nodes(InitNodes) -> init_nodes(InitNodes, []).

init_nodes([NodeArgs | Rest], Nodes) ->
    Host = proplists:get_value(host ,NodeArgs, "127.0.0.1"),
    Port = proplists:get_value(port ,NodeArgs, 6379),
    Password = proplists:get_value(password ,NodeArgs, ""),
    ReconnectSleep = proplists:get_value(reconnect_sleep ,NodeArgs, no_reconnect),
    Size = proplists:get_value(size ,NodeArgs, 10),
    MaxOverflow = proplists:get_value(max_overflow ,NodeArgs, 0),
    Node = #node{
        address = Host,
        port = Port,
        password = Password,
        reconnect_sleep = ReconnectSleep,
        size = Size,
        max_overflow = MaxOverflow
    },
    init_nodes(Rest, [Node | Nodes]);
init_nodes([], Nodes) ->
    Nodes.

%% gen_server.

init([]) ->
    ets:new(?MODULE, [protected, set, named_table,
                      {read_concurrency, true},
                      {keypos, #cluster.cluster_name}]),
    {ok, undefined}.

handle_call({reload_slots_map, ClusterName, Version}, _From, State) ->
    case ets:lookup(?MODULE, ClusterName) of
        [] ->
            ok;
        [#cluster{version = Version} = Cluster] ->
            reload_slots_map(Cluster)
    end,
    {reply, ok, State};
handle_call({connect, ClusterName, InitServers}, _From, State) ->
    connect_(ClusterName, InitServers),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
