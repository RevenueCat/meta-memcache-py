from typing import Callable, Iterable, Optional, Tuple

from meta_memcache.base.base_cache_pool import BaseCachePool
from meta_memcache.commands.high_level_commands import HighLevelCommandsMixin
from meta_memcache.commands.meta_commands import MetaCommandsMixin
from meta_memcache.configuration import (
    ServerAddress,
    build_server_pool,
    default_key_encoder,
)
from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.connection.providers import HashRingConnectionPoolProvider
from meta_memcache.executors.default import DefaultExecutor
from meta_memcache.routers.default import DefaultRouter
from meta_memcache.routers.gutter import GutterRouter
from meta_memcache.interfaces.cache_api import CacheApiProtocol
from meta_memcache.protocol import Key
from meta_memcache.serializer import BaseSerializer, MixedSerializer


class CachePool(HighLevelCommandsMixin, MetaCommandsMixin, BaseCachePool):
    @staticmethod
    def cache_pool_from_servers(
        servers: Iterable[ServerAddress],
        connection_pool_factory_fn: Callable[[ServerAddress], ConnectionPool],
        serializer: Optional[BaseSerializer] = None,
        key_encoder_fn: Callable[[Key], Tuple[bytes, bool]] = default_key_encoder,
        raise_on_server_error: bool = True,
    ) -> CacheApiProtocol:
        executor = DefaultExecutor(
            serializer=serializer or MixedSerializer(),
            key_encoder_fn=key_encoder_fn,
            raise_on_server_error=raise_on_server_error,
        )
        router = DefaultRouter(
            pool_provider=HashRingConnectionPoolProvider(
                server_pool=build_server_pool(servers, connection_pool_factory_fn)
            ),
            executor=executor,
        )
        return CachePool(router=router)

    @staticmethod
    def cache_pool_with_gutter_from_servers(
        servers: Iterable[ServerAddress],
        gutter_servers: Iterable[ServerAddress],
        gutter_ttl: int,
        connection_pool_factory_fn: Callable[[ServerAddress], ConnectionPool],
        serializer: Optional[BaseSerializer] = None,
        key_encoder_fn: Callable[[Key], Tuple[bytes, bool]] = default_key_encoder,
        raise_on_server_error: bool = True,
    ) -> CacheApiProtocol:
        executor = DefaultExecutor(
            serializer=serializer or MixedSerializer(),
            key_encoder_fn=key_encoder_fn,
            raise_on_server_error=raise_on_server_error,
        )
        router = GutterRouter(
            pool_provider=HashRingConnectionPoolProvider(
                server_pool=build_server_pool(servers, connection_pool_factory_fn)
            ),
            gutter_pool_provider=HashRingConnectionPoolProvider(
                server_pool=build_server_pool(
                    gutter_servers, connection_pool_factory_fn
                )
            ),
            gutter_ttl=gutter_ttl,
            executor=executor,
        )
        return CachePool(router=router)
