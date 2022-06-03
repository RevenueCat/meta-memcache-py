# Changelog

## v0.9.2
### Fixed
* `on_write_failure` event handler now can be subscribed/unsubscribed to.

## v0.9.1
* Error logging fixes

## v0.9.0

### Breaking changes
The `WriteFailureTracker` base class has been removed and replaced with the `WriteFailureEvent`.
As a stop-gap, you can apply the following changes to your code:

```diff
from meta_memcache import BaseWriteFailureTracker, Key


- class CacheFailureTracker(BaseWriteFailureTracker):
+ class CacheFailureTracker(object):
+    def __init__(self, pool: BaseCachePool)-> None:
+        self._pool = pool
+        self.start_tracking()
+
+    def start_tracking(self) -> None:
+        self._pool.on_write_failure += self.add_key
+
+    def stop_tracking(self) -> None:
+        self._pool.on_write_failure -= self.add_key

    def add_key(self, key: Key) -> None:
        pass
```

Please note that any existing classes that implement `WriteFailureTracker` should be removed
and the event based solution be properly implemented.
