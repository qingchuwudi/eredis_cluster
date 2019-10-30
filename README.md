# eredis_cluster
[![Travis](https://img.shields.io/travis/adrienmo/eredis_cluster.svg?branch=master&style=flat-square)](https://travis-ci.org/adrienmo/eredis_cluster)
[![Hex.pm](https://img.shields.io/hexpm/v/eredis_cluster.svg?style=flat-square)](https://hex.pm/packages/eredis_cluster)

## Description

eredis_cluster is a wrapper for eredis to support cluster mode of redis 3.0.0+

## TODO

- Improve test suite to demonstrate the case where redis cluster is crashing,
resharding, recovering...

## Compilation && Test

The directory contains a Makefile and rebar3

	make
	make test

## Configuration

支持多集群以后不再使用默认配置。自此redis_cluster配置项取消，需要引用者自行指定。

配置格式：
```erlang
[
    {cluster_name, ClusterName::atom()}, % 集群连接池名字
    {nodes, [ % 同一个集群中的节点，配置多个可以保证在某一个不可用的情况下集群仍然可以连接
        [{host, Host :: string()}, {port, Port :: non_neg_integer()}],
        [{host, Host :: string()}, {port, Port :: non_neg_integer()}],
        [{host, Host :: string()}, {port, Port :: non_neg_integer()}]
    ]},
    {password, Password :: string()}, % redis密码
    {size, Size :: non_neg_integer()}, % 集群每个主节点的连接数
    {max_overflow, MaxOverflow :: non_neg_integer()} % 集群每个主节点可以溢出的连接数
]
```

示例：

```erlang

application:ensure_all_started(eredis).
application:ensure_all_started(eredis_cluster).

ClusterOptions = [
    {cluster_name, 'test'},
    {nodes, [
        [{host, "10.0.105.221"}, {port, 7003}],
        [{host, "10.0.105.222"}, {port, 7004}],
        [{host, "10.0.105.223"}, {port, 7004}]
    ]},
    {password, ""},
    {size, 5},
    {max_overflow, 0}
].

eredis_cluster:connect(ClusterOptions).

eredis_cluster:q(test, ["cluster", "slots"]).

eredis_cluster:connect(test2, ClusterOptions).
eredis_cluster:q(test2, ["cluster", "slots"]).
```

## Usage

```erlang
%% Start the application
eredis_cluster:start().

%% Simple command
eredis_cluster:q(["GET","abc"]).

%% Pipeline
eredis_cluster:qp([["LPUSH", "a", "a"], ["LPUSH", "a", "b"], ["LPUSH", "a", "c"]]).

%% Pipeline in multiple node (keys are sorted by node, a pipeline request is
%% made on each node, then the result is aggregated and returned. The response
%% keep the command order
eredis_cluster:qmn([["GET", "a"], ["GET", "b"], ["GET", "c"]]).

%% Transaction
eredis_cluster:transaction([["LPUSH", "a", "a"], ["LPUSH", "a", "b"], ["LPUSH", "a", "c"]]).

%% Transaction Function
Function = fun(Worker) ->
    eredis_cluster:qw(Worker, ["WATCH", "abc"]),
    {ok, Var} = eredis_cluster:qw(Worker, ["GET", "abc"]),

    %% Do something with Var %%
    Var2 = binary_to_integer(Var) + 1,

    {ok, Result} = eredis_cluster:qw(Worker,[["MULTI"], ["SET", "abc", Var2], ["EXEC"]]),
    lists:last(Result)
end,
eredis_cluster:transaction(Function, "abc").

%% Optimistic Locking Transaction
Function = fun(GetResult) ->
    {ok, Var} = GetResult,
    Var2 = binary_to_integer(Var) + 1,
    {[["SET", Key, Var2]], Var2}
end,
Result = optimistic_locking_transaction(Key, ["GET", Key], Function),
{ok, {TransactionResult, CustomVar}} = Result

%% Atomic Key update
Fun = fun(Var) -> binary_to_integer(Var) + 1 end,
eredis_cluster:update_key("abc", Fun).

%% Atomic Field update
Fun = fun(Var) -> binary_to_integer(Var) + 1 end,
eredis_cluster:update_hash_field("abc", "efg", Fun).

%% Eval script, both script and hash are necessary to execute the command,
%% the script hash should be precomputed at compile time otherwise, it will
%% execute it at each request. Could be solved by using a macro though.
Script = "return redis.call('set', KEYS[1], ARGV[1]);",
ScriptHash = "4bf5e0d8612687699341ea7db19218e83f77b7cf",
eredis_cluster:eval(Script, ScriptHash, ["abc"], ["123"]).

%% Flush DB
eredis_cluster:flushdb().

%% Query on all cluster server
eredis_cluster:qa(["FLUSHDB"]).

%% Execute a query on the server containing the key "TEST"
eredis_cluster:qk(["FLUSHDB"], "TEST").
```
