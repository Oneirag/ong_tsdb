import os

import numpy as np
from ong_utils import OngConfig, LOCAL_TZ

__version__ = "0.7.2"  # Version of the software

__cfg = OngConfig("ong_tsdb")
config = __cfg.config
logger = __cfg.logger
if not config("BASE_DIR", None):
    print("BASE_DIR config variable is not set. Using ~/ong_tsdb as default")
BASE_DIR = os.path.expanduser(config("BASE_DIR", "~/ong_tsdb"))
COMPRESSION_EXT = ".gz"
# DTYPE = np.float32  # Dates would end if 2038 if used float32
DTYPE = np.float64
DTYPE = np.float32  # Now trying storing not the full TS but the offset with the TS start...
CHUNK_ROWS = 2 ** 14
HTTP_COMPRESS_THRESHOLD = 1024  # Minimum number of data to activate compression in client and server
HELLO_MSG = f"Hello from Ong_Tsdb server\nVersion {__version__}"
