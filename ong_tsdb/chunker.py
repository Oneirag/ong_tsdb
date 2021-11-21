"""
Class to manage chunk files
A chunk is a file
"""
import time

import numpy as np

from ong_tsdb import COMPRESSION_EXT, DTYPE, config, CHUNK_ROWS
from ong_tsdb.fileutils import generate_filename_from_parts


class Chunker(object):
    """
    Manages the chunk files: file size, start date, position in the file...
    """

    def __init__(self, freq, retention_chunks=None):
        """
        Initializes Chunker, a class to decide in which filename a timestamp should go
        Raises Exception if frequency is not implemented

        :param freq: the frequency of the tick data in seconds (can be a float value).
                Can use "s" for seconds, m for minutes, h for hour and d for days
                Example: "3m" and 180 means the same
        :param retention_chunks: number of recent chunks that should not be compressed
        """
        self.n_rows_per_chunk = CHUNK_ROWS
        self.itemsizebytes = np.dtype(DTYPE).itemsize
        if isinstance(freq, str):
            period_type = freq[-1].lower()
            period_length = float(freq[:-1])
            if period_type == "s":
                multiplier = 1
            elif period_type == "m":
                multiplier = 60
            elif period_type == "h":
                multiplier = 60 * 60
            elif period_type == "d":
                multiplier = 60 * 60 * 24       # TODO: won't work properly in case of day light saving time changes
            else:
                raise Exception("Frequency: " + freq + " not implemented")
            self.tick_duration = period_length * multiplier
        else:
            self.tick_duration = float(freq)
        self.chunk_duration = self.n_rows_per_chunk * self.tick_duration
        self.retention_policy_chunks = retention_chunks if retention_chunks is not None else \
            int(config('uncompressed_chunks', -1))

    def compressed_by_policy(self, date_ts: float) -> bool:
        """True if the chunk corresponds to a chunk that has to be compressed"""
        if self.retention_policy_chunks is None or self.retention_policy_chunks < 0:
            return False
        else:
            return ((time.time() - date_ts) / self.chunk_duration) > self.retention_policy_chunks

    def chunk_timestamp(self, timestamp_ms):
        """Returns the timestamp of the chunk corresponding to the given timestamp (in millis)"""
        return int(timestamp_ms / self.chunk_duration) * self.chunk_duration

    def chunk_name(self, timestamp, n_columns, compressed=None):
        if compressed is None:
            compressed = (time.time() - timestamp) > self.retention_policy_chunks * self.chunk_duration
        return generate_filename_from_parts(path="",
                                            timestamp=self.chunk_timestamp(timestamp),
                                            n_columns=n_columns,
                                            compression=COMPRESSION_EXT if compressed else "")

    def getpos(self, timestamp):
        if isinstance(timestamp, np.ndarray):
            to_int = np.vectorize(np.int)
            return to_int((timestamp - self.chunk_timestamp(timestamp[0])) / self.tick_duration)
        else:
            return int((timestamp - self.chunk_timestamp(timestamp)) / self.tick_duration)

    def np_shape(self, columns):
        """
        Returns a tuple with the size of the ndarray stored in the chunk
        Args:
            columns -- int
                number of data columns in the chunk
        """
        return self.n_rows_per_chunk, columns + 2

    def file_size(self, columns):
        """
        Returns the chunk file size in bytes
        Args:
            columns -- int
                number of data columns in the chunk
        """
        shape = self.np_shape(columns)
        return self.itemsizebytes * shape[0] * shape[1]
