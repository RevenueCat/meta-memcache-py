# Define how long a server is marked down after a failure.
# While marked down, connection is not retried, so latency
# is not that impacted by unresponsive servers.
DEFAULT_MARK_DOWN_PERIOD_S = 5.0

DEFAULT_READ_BUFFER_SIZE = 4096

# Max key is 250, but when using binary keys will be b64 encoded
# so take more space. Keys longer than this will be hashed, so
# it's not a problem.
MAX_KEY_SIZE = 187
