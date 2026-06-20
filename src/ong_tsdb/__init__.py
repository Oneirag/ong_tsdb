import os

import numpy as np
from ong_utils import OngConfig, LOCAL_TZ

__version__ = "0.9.3"  # Version of the software

__cfg = OngConfig("ong_tsdb")
config = __cfg.config
logger = __cfg.logger
if not config("BASE_DIR", None):
    print("BASE_DIR config variable is not set. Using ~/ong_tsdb as default")
BASE_DIR = os.path.expanduser(config("BASE_DIR", "~/ong_tsdb"))

# Compression extensions. Both formats are supported for reading; new
# chunks are written with the default (zstd) since 0.9.0 because the
# benchmark showed ~25% better ratio and 4-12x faster compress/
# decompress than gzip on representative data. Old gzip chunks keep
# working transparently and migrate naturally as they are rewritten.
COMPRESSION_GZIP = ".gz"
COMPRESSION_ZSTD = ".zst"
# Default for new writes. Reading is extension-driven so the value
# here does not need to match what is on disk.
COMPRESSION_EXT = COMPRESSION_ZSTD

DTYPE = (
    np.float32
)  # Timestamps are stored as offset, not as absolute dates, so 2038 is not a concern.
CHUNK_ROWS = 2**14
HTTP_COMPRESS_THRESHOLD = (
    1024  # Minimum number of data to activate compression in client and server
)
HELLO_MSG = f"Hello from Ong_Tsdb server\nVersion {__version__}"
