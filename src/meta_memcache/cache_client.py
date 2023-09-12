from typing import Callable, Iterable, Optional, Tuple

from meta_memcache.base.base_cache_client import BaseCacheClient
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
from meta_memcache.routers.ephemeral import EphemeralRouter
from meta_memcache.routers.gutter import GutterRouter
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.protocol import Key
from meta_memcache.serializer import BaseSerializer, MixedSerializer


class CacheClient(HighLevelCommandsMixin, MetaCommandsMixin, BaseCacheClient):
    @staticmethod
    def cache_client_from_servers(
        servers: Iterable[ServerAddress],
        connection_pool_factory_fn: Callable[[ServerAddress], ConnectionPool],
        serializer: Optional[BaseSerializer] = None,
        key_encoder_fn: Callable[[Key], Tuple[bytes, bool]] = default_key_encoder,
        raise_on_server_error: bool = True,
    ) -> CacheApi:
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
        return CacheClient(router=router)

    @staticmethod
    def cache_client_with_gutter_from_servers(
        servers: Iterable[ServerAddress],
        gutter_servers: Iterable[ServerAddress],
        gutter_ttl: int,
        connection_pool_factory_fn: Callable[[ServerAddress], ConnectionPool],
        serializer: Optional[BaseSerializer] = None,
        key_encoder_fn: Callable[[Key], Tuple[bytes, bool]] = default_key_encoder,
        raise_on_server_error: bool = True,
    ) -> CacheApi:
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
        return CacheClient(router=router)

    @staticmethod
    def ephemeral_cache_client_from_servers(
        servers: Iterable[ServerAddress],
        max_ttl: int,
        connection_pool_factory_fn: Callable[[ServerAddress], ConnectionPool],
        serializer: Optional[BaseSerializer] = None,
        key_encoder_fn: Callable[[Key], Tuple[bytes, bool]] = default_key_encoder,
        raise_on_server_error: bool = True,
    ) -> CacheApi:
        executor = DefaultExecutor(
            serializer=serializer or MixedSerializer(),
            key_encoder_fn=key_encoder_fn,
            raise_on_server_error=raise_on_server_error,
        )
        router = EphemeralRouter(
            max_ttl=max_ttl,
            pool_provider=HashRingConnectionPoolProvider(
                server_pool=build_server_pool(servers, connection_pool_factory_fn)
            ),
            executor=executor,
        )
        return CacheClient(router=router)
