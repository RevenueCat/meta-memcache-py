from typing import Protocol

from meta_memcache.interfaces.high_level_commands import HighLevelCommandsProtocol
from meta_memcache.interfaces.meta_commands import MetaCommandsProtocol


class CommandsProtocol(HighLevelCommandsProtocol, MetaCommandsProtocol, Protocol):
    pass
