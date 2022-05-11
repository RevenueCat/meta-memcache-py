class MemcacheError(Exception):
    pass


class MemcacheServerError(MemcacheError):
    def __init__(self, server: str, message: str) -> None:
        self.server = server
        super().__init__(message)
