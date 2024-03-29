# meta-memcache-py
Modern, pure python, memcache client with support for new meta commands.

# Usage:

## Install:
```
pip install meta-memcache
```
## Configure a client:
```python:
from meta_memcache import (
    Key,
    ServerAddress,
    CacheClient,
    connection_pool_factory_builder,
)

pool = CacheClient.cache_client_from_servers(
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

The `CacheClient.cache_client_from_servers`s expects a `connection_pool_factory_fn`
callback to build the internal connection pool. And the connection pool receives
a function to create a new memcache connection.

While this is very flexible, it can be complex to initialize, so there is a
default builder provided to tune the most frequent things:
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
* `max_pool_size`: Maximum number of connections to keep open for each host in
  the pool. Note that if there are no connections available in the pool, the
  client will open a new connection always, instead of just blocking waiting
  for a free connection. If you see too many connection creations in the
  stats, you might need to increase this setting.
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
cache_client.set(key=Key("bar"), value=1, ttl=1000)
```

## Keys:
### String or `Key` named tuple
On the high-level commands you can use either plain strings as keys
or the more advanced `Key` object that allows extra features like
custom routing and unicode keys.

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
goes down, the user life will be miserable.

### Custom domains:
You can add a domain to keys. This domain can be used for custom per-domain
metrics like hit ratios or to control serialization of the values.
```python:
Key("key:1:2", domain="example")
```
For example the ZstdSerializer allows to configure different dictionaries by
domain, so you can compress more efficiently data of different domains.

### Unicode/binary keys:
Both unicode and binary keys are supported, the keys will be hashed/encoded according to Meta commands
[binary encoded keys](https://github.com/memcached/memcached/wiki/MetaCommands#binary-encoded-keys)
specification.

Using binary keys can have benefits, saving space in memory. While over the wire the key
is transmited b64 encoded, the memcache server will use the byte representation, so it will
not have the 1/4 overhead of b64 encoding.

```python:
Key("🍺")
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

## Architecture
Code should rely on the [`CacheApi`](https://github.com/RevenueCat/meta-memcache-py/blob/main/src/meta_memcache/interfaces/cache_api.py) prototol.

The client itself is just two mixins that implement meta-commands and high-level commands.

The client uses a `Router` to execute the commands. Routers are reponsible of routing
the request to the appropriate place.

By default a client has a single Cache pool defined, with each of the servers
having its own connection pool. The Router will map the key to the appropriate
server's connection pool. It can also implement more complex routing policies
like shadowing and mirroring.

The Router uses the connection pool and the executor to execute the command and read the
response.

By mixing and plugging routers and executors is possible to build advanced
behaviors. Check [`CacheClient`](https://github.com/RevenueCat/meta-memcache-py/blob/main/src/meta_memcache/cache_client.py) and [`extras/`](https://github.com/RevenueCat/meta-memcache-py/blob/main/src/meta_memcache/extrasy) for some examples.

## Low level meta commands:

The low-level commands are in
[BaseCacheClient](https://github.com/RevenueCat/meta-memcache-py/blob/main/src/meta_memcache/commands/meta_commands.py).

They implement the new
[Memcache MetaCommands](https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L79):
meta get, meta set, meta delete and meta arithmetic. They implement the full set
of flags, and features, but are very low level for general use.

```python:
    def meta_multiget(
        self,
        keys: List[Key],
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> Dict[Key, ReadResponse]:

    def meta_get(
        self,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> ReadResponse:

    def meta_set(
        self,
        key: Key,
        value: Any,
        ttl: int,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:

    def meta_delete(
        self,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:

    def meta_arithmetic(
        self,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
```
### Special arguments:
`RequestFlags` has the following arguments:
 * `no_reply`: Set to True if the server should not send a response
 * `return_client_flag`: Set to True if the server should return the client flag
 * `return_cas_token`: Set to True if the server should return the CAS token
 * `return_value`: Set to True if the server should return the value (Default)
 * `return_ttl`: Set to True if the server should return the TTL
 * `return_size`: Set to True if the server should return the size (useful if when paired with return_value=False, to get the size of the value)
 * `return_last_access`: Set to True if the server should return the last access time
 * `return_fetched`: Set to True if the server should return the fetched flag
 * `return_key`: Set to True if the server should return the key in the response
 * `no_update_lru`: Set to True if the server should not update the LRU on this access
 * `mark_stale`: Set to True if the server should mark the value as stale
 * `cache_ttl`: The TTL to set on the key
 * `recache_ttl`: The TTL to use for recache policy
 * `vivify_on_miss_ttl`: The TTL to use when vivifying a value on a miss
 * `client_flag`: The client flag to store along the value (Useful to store value type, compression, etc)
 * `ma_initial_value`: For arithmetic operations, the initial value to use (if the key does not exist)
 * `ma_delta_value`: For arithmetic operations, the delta value to use
 * `cas_token`: The CAS token to use when storing the value in the cache
 * `opaque`: The opaque flag (will be echoed back in the response)
 * `mode`: The mode to use when storing the value in the cache. See SET_MODE_* and MA_MODE_* constants

`FailureHandling` controls how the failures are handled. Has the arguments:
 * `raise_on_server_error`: (`Optional[bool]`) Wether to raise on error:
   - `True`: Raises on server errors
   - `False`: Returns miss for reads and false on writes
   - `None` (DEFAULT): Use the raise on error setting configured in the Router
 * `track_write_failures``: (`bool`) Wether to track failures:
   - `True` (DEFAULT): Track write failures
   - `False`: Do not notify write failures

The default settings are usually good, but there are situations when you want control.
For example, a refill (populating an entry that was missing on cache) doesn't need to
track write failures. If fails to be written, the cache will still be empty, so no need
to track that as a write failure. Similarly sometimes you need to know if a write failed
due to CAS semantics, or because it was an add vs when it is due to server failure.

### Responses:
The responses are either:
 * `ReadResponse`: `Union[Miss, Value, Success]`
 * `WriteResponse`:  `Union[Success, NotStored, Conflict, Miss]`

Which are:
 * `Miss`: For key not found. No arguments
 * `Success`: Successfull operation
   - `flags`: `ResponseFlags` 
 * `Value`: For value responses
   - `flags`: `ResponseFlags` 
   - `size`: `int` Size of the value
   - `value`: `Any` The value
 * `NotStored`: Not stored, for example "add" on exising key. No arguments.
 * `Conflict`: Not stored, for example due to CAS mismatch. No arguments.

The `ResponseFlags` contains the all the returned flags. This metadata gives a lot of
control and posibilities, it is the strength of the meta protocol:
 * `cas_token`: Compare-And-Swap token (integer value) or `None` if not returned
 * `fetched`:
     - `True` if fetched since being set
     - `False` if not fetched since being set
     - `None` if the server did not return this flag info
 * `last_access`: time in seconds since last access (integer value) or `None` if not returned
 * `ttl`: time in seconds until the value expires (integer value) or `None` if not returned
     - The special value `-1` represents if the key will never expire
 * `client_flag`: integer value or `None` if not returned
 * `win`:
     - `True` if the client won the right to repopulate
     - `False` if the client lost the right to repopulate
     - `None` if the server did not return a win/lose flag
 * `stale`: `True` if the value is stale, `False` otherwise
 * `real_size`: integer value or `None` if not returned
 * `opaque flag`: bytes value or `None` if not returned

NOTE: You shouldn't use this api directly, unless you are implementing some custom high-level
command. See below for the usual memcache api.

## High level commands:

The
[High level commands](https://github.com/RevenueCat/meta-memcache-py/blob/main/src/meta_memcache/commands/high_level_commands.py)
augments the low-level api and implement the more usual, high-level memcache
operations (get, set, touch, cas...), plus the memcached's
[new MetaCommands anti-dogpiling techniques](https://github.com/memcached/memcached/wiki/MetaCommands)
for high qps caching needs: Atomic Stampeding control, Herd Handling, Early
Recache, Serve Stale, No Reply, Probabilistic Hot Cache, Hot Key Cache
Invalidation...

```python:
    def set(
        self,
        key: Union[Key, str],
        value: Any,
        ttl: int,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
        stale_policy: Optional[StalePolicy] = None,
        set_mode: SetMode = SetMode.SET,  # Other are ADD, REPLACE, APPEND...
    ) -> bool:
        """
        Write a value using the specified `set_mode`
        """

    def refill(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        value: Any,
        ttl: int,
        no_reply: bool = False,
    ) -> bool:
        """
        Try to refill a value.

        Use this method when you got a cache miss, read from DB and
        are trying to refill the value.

        DO NOT USE to write new state.

        It will:
         * use "ADD" mode, so it will fail if the value is already
           present in cache.
         * It will also disable write failure tracking. The write
           failure tracking is often used to invalidate keys that
           fail to be written. Since this is not writting new state,
           there is no need to track failures.
        """

    def delete(
        self,
        key: Union[Key, str],
        cas_token: Optional[int] = None,
        no_reply: bool = False,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:
        """
        Returns True if the key existed and it was deleted.
        If the key is not found in the cache it will return False. If
        you just want to the key to be deleted not caring of whether
        it exists or not, use invalidate() instead.
        """

    def invalidate(
        self,
        key: Union[Key, str],
        cas_token: Optional[int] = None,
        no_reply: bool = False,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:
        """
        Returns true of the key deleted or it didn't exist anyway
        """

    def touch(
        self,
        key: Union[Key, str],
        ttl: int,
        no_reply: bool = False,
    ) -> bool:
        """
        Modify the TTL of a key without retrieving the value
        """

    def get_or_lease(
        self,
        key: Union[Key, str],
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        """
        Get a key. On miss try to get a lease.

        Guarantees only one cache client will get the miss and
        gets to repopulate cache, while the others are blocked
        waiting (according to the settings in the LeasePolicy)
        """

    def get_or_lease_cas(
        self,
        key: Union[Key, str],
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        """
        Same as get_or_lease(), but also return the CAS token so
        it can be used during writes and detect races
        """

    def get(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        """
        Get a key
        """

    def multi_get(
        self,
        keys: List[Union[Key, str]],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Dict[Key, Optional[Any]]:
        """
        Get multiple keys at once
        """

    def get_cas(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        """
        Same as get(), but also return the CAS token so
        it can be used during writes and detect races
        """

    def get_typed(
        self,
        key: Union[Key, str],
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Optional[T]:
        """
        Same as get(), but ensure the type matched the provided cls
        """

    def get_cas_typed(
        self,
        key: Union[Key, str],
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Tuple[Optional[T], Optional[int]]:
        """
        Same as get_typed(), but also return the CAS token so
        it can be used during writes and detect races
        """

    def delta(
        self,
        key: Union[Key, str],
        delta: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
    ) -> bool:
        """
        Increment/Decrement a key that contains a counter
        """

    def delta_initialize(
        self,
        key: Union[Key, str],
        delta: int,
        initial_value: int,
        initial_ttl: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
    ) -> bool:
        """
        Increment/Decrement a key that contains a counter,
        creating and setting it to the initial value if the
        counter does not exist.
        """

    def delta_and_get(
        self,
        key: Union[Key, str],
        delta: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:
        """
        Same as delta(), but return the resulting value
        """

    def delta_initialize_and_get(
        self,
        key: Union[Key, str],
        delta: int,
        initial_value: int,
        initial_ttl: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:
        """
        Same as delta_initialize(), but return the resulting value
        """
```

# Reliability, consistency and best practices
We have published a deep-dive into some of the techniques to keep
cache consistent and reliable under high load that RevenueCat uses,
available thanks to this cache client.

See: https://www.revenuecat.com/blog/engineering/data-caching-revenuecat/

## Anti-dogpiling, preventing thundering herds:
Some commands receive `RecachePolicy`, `StalePolicy` and `LeasePolicy` for the
advanced anti-dogpiling control needed in high-qps environments:

```python:
class RecachePolicy(NamedTuple):
    """
    This controls the recache herd control behavior
    If recache ttl is indicated, when remaining ttl is < given value
    one of the clients will win, return a miss and will populate the
    value, while the other clients will loose and continue to use the
    stale value.
    """

    ttl: int = 30


class LeasePolicy(NamedTuple):
    """
    This controls the lease or miss herd control behavior
    If miss lease retries > 0, on misses a lease will be created. The
    winner will get a Miss and will continue to populate the cache,
    while the others are BLOCKED! Use with caution! You can define
    how many times and how often clients will retry to get the
    value. After the retries are expired, clients will get a Miss
    if they weren't able to get the value.
    """

    ttl: int = 30
    miss_retries: int = 3
    miss_retry_wait: float = 1.0
    wait_backoff_factor: float = 1.2
    miss_max_retry_wait: float = 5.0


class StalePolicy(NamedTuple):
    """
    This controls the stale herd control behavior
    * Deletions can mark items stale instead of deleting them
    * Stale items automatically do recache control, one client
      will get the miss, others will receive the stale value
      until the winner refreshes the value in the cache.
    * cas mismatches (due to race / further invalidation) can
      store the value as stale instead of failing
    """

    mark_stale_on_deletion_ttl: int = 0  # 0 means disabled
    mark_stale_on_cas_mismatch: bool = False
``` 

### Notes:
* Recache/Stale policies are typically used together. Make sure all your reads
  for a given key share the same recache policy to avoid unexpected behaviors.
* Leases are for a more traditional, more consistent model, where other clients
  will block instead of getting a stale value.

## Pool level features:
Finally in
[`cache_client.py`](https://github.com/RevenueCat/meta-memcache-py/blob/main/src/meta_memcache/cache_client.py)
helps building different `CacheClient`s with different characteristics:
* `cache_client_from_servers`: Implements the default, shardedconsistent hashing
  cache pool using uhashring's `HashRing`.
* `cache_client_with_gutter_from_servers`: implements a sharded cache pool like
  above, but with a 'gutter pool' (See
  [Scaling Memcache at Facebook](http://www.cs.utah.edu/~stutsman/cs6963/public/papers/memcached.pdf)),
  so when a server of the primary pool is down, requests are sent to the
  'gutter' pool, with TTLs overriden and lowered on the fly, so they provide
  some level of caching instead of hitting the backend for each request.

These clients also provide an option to register a callback for write failure events. This might be useful
if you are serious about cache consistency. If you have transient network issues, some writes might fail,
and if the server comes back without being restarted or the cache flushed, the data will be stale. The
events allows for failed writes to be collected and logged. Affected keys can then be invalidated later
and eventual cache consistency guaranteed.

It should be trivial to implement your own cache client if you need custom
sharding, shadowing, pools that support live migrations, etc. Feel free to
contribute!

## Write failure tracking
When a write failure occures with a `SET` or `DELETE` opperation occures then the `cache_client.on_write_failure` event
handler will be triggered. Consumers subscribing to this handler will receive the key that failed. The
following is an example on how to subscribe to these events:
```python:
from meta_memcache import CacheClient, Key

class SomeConsumer(object):
    def __init__(self, cache_client: CacheClient):
        self.cache_client = cache_client
        self.cache_client.on_write_failures += self.on_write_failure_handler

    def call_before_dereferencing(self):
        self.cache_client.on_write_failures -= self.on_write_failure_handler

    def on_write_failure_handler(self, key: Key) -> None:
        # Handle the failures here
        pass
```

Ideally you should track such errors and attempt to invalidate the affected keys
later, when the cache server is back, so you keep high cache consistency, avoiding
stale entries.

## Stats:
The cache clients offer a `get_counters()` that return information about the state
of the servers and their connection pools:

```python:
    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
```

The counters are:
```python:
class PoolCounters(NamedTuple):
    # Available connections in the pool, ready to use
    available: int
    # The # of connections active, currently in use, out of the pool
    active: int
    # Current stablished connections (available + active)
    stablished: int
    # Total # of connections created. If this keeps growing
    # might mean the pool size is too small and we are
    # constantly needing to create new connections:
    total_created: int
    # Total # of connection or socket errors
    total_errors: int
```

