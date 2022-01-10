# meta-memcache-py
Modern, pure python, memcache client with support for new meta commands.

# Usage:

## Install:
```
pip install meta-memcache
```
## Configure a pool:
```python:
from meta_memcache import (
    Key,
    ServerAddress,
    ShardedCachePool,
    connection_pool_factory_builder,
)

pool = ShardedCachePool.from_server_addresses(
    servers=[
        ServerAddress(host="1.1.1.1", port=11211),
        ServerAddress(host="2.2.2.2", port=11211),
        ServerAddress(host="3.3.3.3", port=11211),
    ],
    connection_pool_factory_fn=connection_pool_factory_builder(),
)
```

The design is very pluggable. Rather than supporting a lot of features, it
relies on dependency injection to configure behavior.

The `CachePool`s expect a `connection_pool_factory_fn` callback to build the
internal connection pool. There is a default builder provided to tune the most
frequent things:
```
def connection_pool_factory_builder(
    initial_pool_size: int = 1,
    max_pool_size: int = 3,
    mark_down_period_s: float = DEFAULT_MARK_DOWN_PERIOD_S,
    connection_timeout: float = 1,
    recv_timeout: float = 1,
    no_delay: bool = True,
    read_buffer_size: int = 4096,
) -> Callable[[ServerAddress], ConnectionPool]:
```
* `initial_pool_size`: How many connections open for each host in the pool
* `max_pool_size`: Maximim number of connections to keep open for each host in
  the pool. Note that if there are no connections available in the pool, the
  client will open a new connection always instead of of just blocking waiting
  for a free connection.
* `mark_down_period_s`: When a network failure is detected, the destination host
  is marked down, and requests will fail fast, instead of trying to reconnect
  causing clients to block. A single client request will be checking if the host
  is still down every `mark_down_period_s`, while the others fail fast.
* `connection_timeout`: Timeout to stablish initial connection, ideally should
  be < 1 s for memcache servers in local network.
* `recv_timeout`: Timeout of requests. Ideally should be < 1 s for memcache
  servers in local network.
* `no_delay`: Wether to configure socket with NO_DELAY. This library tries to
  send requests as a single write(), so enabling no_delay is a good idea.
* `read_buffer_size`: This client tries to minimize memory allocation by reading
  bytes from the socket into a reusable read buffer. If the memcache response
  size is < `read_buffer_size` no memory allocations happen for the network
  read. Note: Each connection will have its own read buffer, so you must find a
  good balance between memory usage and reducing memory allocations. Note: While
  reading from the socket has zero allocations, the values will be deserialized
  and those will have the expected memory allocations.

If you need to customize how the sockets are created (IPv6, add auth, unix
sockets) you will need to implement your own `connection_pool_factory_builder`
and override the `socket_factory_fn`.

## Use the pool:
```python:
cache_pool.set(key=Key("bar"), value=1, ttl=1000)
```

## Keys:

### Custom routing:
You can control the routing of the keys setting a custom `routing_key`:
```python:
Key("key:1:2", routing_key="key:1")
```
This is useful if you have several keys related with each other. You can use the
same routing key, and they will be placed in the same server. This is handy for
speed of retrieval (single request instead of fan-out) and/or consistency (all
will be gone or present, since they are stored in the same server). Note this is
also risky, if you place all keys of a user in the same server, and the server
goes down, the user life will be misserable.

### Unicode keys:
Unicode keys are supported, the keys will be hashed according to Meta commands
[binary encoded keys](https://github.com/memcached/memcached/wiki/MetaCommands#binary-encoded-keys)
specification.

To use this, mark the key as unicode:
```python:
Key("🍺", unicode=True)
```

### Large keys:
Large keys are also automatically supported and binary encoded as above. But
don't use large keys :)

# Design:
The code relies on dependency injection to allow to configure a lot of the
aspects of the cache client. For example, instead of supporting a lot of
features on how to connect, authenticate, etc, a `socket_factory_fn` is required
and you can customize the socket creation to your needs. We provide some basic
sane defaults, but you should not have a lot of issues to customize it for your
needs.

Regarding cache client features, relies in inheritance to abstract different
layers of responsibility, augment the capabilities while abstracting details
out:

## Low level meta commands:

The low-level class is
[BaseCachePool](https://github.com/RevenueCat/meta-memcache-py/blob/main/src/meta_memcache/base/base_cache_pool.py).
Implements the connection pool handling as well as the memcache protocol,
focusing on the new
[Memcache MetaCommands](https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L79):
meta get, meta set, meta delete and meta arithmetic. They implement the full set
of flags, and features, but are very low level. You won't use this unless you
are implementing some custom high-level command. See below for the usual
memcache api.

## High level commands:

The
[CachePool](https://github.com/RevenueCat/meta-memcache-py/blob/main/src/meta_memcache/base/cache_pool.py)
augments the low-level class and implements the more high-level memcache
operations (get, set, touch, cas...), plus the
[new meta memcache goodies](https://github.com/memcached/memcached/wiki/MetaCommands)
for high qps caching needs: Atomic Stampeding control, Herd Handling, Early
Recache, Serve Stale, No Reply, Probabilistic Hot Cache, Hot Key Cache
Invalidation...

```python:
    def set(
        self,
        key: Key,
        value: Any,
        ttl: int,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:

    def delete(
        self,
        key: Key,
        cas_token: Optional[int] = None,
        no_reply: bool = False,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:

    def touch(
        self,
        key: Key,
        ttl: int,
        no_reply: bool = False,
    ) -> bool:

    def get_or_lease(
        self,
        key: Key,
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:

    def get_or_lease_cas(
        self,
        key: Key,
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:

    def get(
        self,
        key: Key,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:

    def multi_get(
        self,
        keys: List[Key],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Dict[Key, Optional[Any]]:

    def get_cas(
        self,
        key: Key,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:

    def get_typed(
        self,
        key: Key,
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Optional[T]:

    def get_cas_typed(
        self,
        key: Key,
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Tuple[Optional[T], Optional[int]]:

    def _get_delta_flags(
        self,
        delta: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
        return_value: bool = False,
    ) -> Tuple[Set[Flag], Dict[IntFlag, int], Dict[TokenFlag, bytes]]:

    def delta(
        self,
        key: Key,
        delta: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
    ) -> bool:

    def delta_initialize(
        self,
        key: Key,
        delta: int,
        initial_value: int,
        initial_ttl: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
    ) -> bool:

    def delta_and_get(
        self,
        key: Key,
        delta: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:

    def delta_initialize_and_get(
        self,
        key: Key,
        delta: int,
        initial_value: int,
        initial_ttl: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:
```

## Pool level features:
Finally in
[`cache_pools.py`](https://github.com/RevenueCat/meta-memcache-py/blob/main/src/meta_memcache/cache_pools.py)
a few classes implement the pool-level semantics:
* `ShardedCachePool`: implements a consistent hashing cache pool using uhashring's `HashRing`.
* `ShardedWithGutterCachePool`: implements a sharded cache pool like above, but
  with a 'gutter pool' (See
  [Scaling Memcache at Facebook](http://www.cs.utah.edu/~stutsman/cs6963/public/papers/memcached.pdf)),
  so when a server of the primary pool is down, requests are sent to the
  'gutter' pool, with TTLs overriden and lowered on the fly, so they provide
  some level of caching instead of hitting the backend for each request.

These pools also provide a write failure tracker feature, usefull if you are
serious about cache consistency. If you have transient network issues, some
writes might fail, and if the server comes back without being restarted or the
cache flushed, the data will be stale. This allows for failed writes to be
collected and logged. Affected keys can then be invalidated later and eventual
cache consistency guaranteed.

It should be trivial to implement your own cache pool if you need custom
sharding, shadowing, pools that support live migrations, etc. Feel free to
contribute!