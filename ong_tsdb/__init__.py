import os

import numpy as np

from ong_utils import LOCAL_TZ
from ong_utils.config import OngConfig

__cfg = OngConfig("ong_tsdb")
config = __cfg.config
logger = __cfg.logger
BASE_DIR = os.path.expanduser(config("BASE_DIR"))
COMPRESSION_EXT = ".gz"
# DTYPE = np.float32  # Dates would end if 2038 if used float32
DTYPE = np.float64
DTYPE = np.float32  # Now trying storing not the full TS but the offset with the TS start...
CHUNK_ROWS = 2 ** 14
HELLO_MSG = "Hello from Ong_Tsdb server"
