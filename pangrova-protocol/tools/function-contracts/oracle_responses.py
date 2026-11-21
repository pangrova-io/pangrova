"""Bounded line transport for the actual Native resolver differential tool."""

import math
import os
import selectors
import time


class OracleResponses:
    def __init__(self, stream, timeout_seconds=30, max_bytes=4 * 1024 * 1024):
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 60):
            raise ValueError("Oracle response timeout must be finite and in (0, 60]")
        if type(max_bytes) is not int or not 1 <= max_bytes <= 4 * 1024 * 1024:
            raise ValueError("Oracle response byte bound must be in [1, 4194304]")
        self.timeout_seconds = timeout_seconds
        self.max_bytes = max_bytes
        self.pending = bytearray()
        self.selector = selectors.DefaultSelector()
        try:
            self.selector.register(stream, selectors.EVENT_READ)
        except BaseException:
            self.selector.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.selector.close()

    def next_line(self):
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            end = self.pending.find(b"\n")
            if end >= 0:
                line = bytes(self.pending[:end + 1])
                del self.pending[:end + 1]
                return line.decode("utf-8", errors="strict")
            if len(self.pending) >= self.max_bytes:
                raise ValueError("Actual Native oracle exceeded its response byte bound")
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not (ready := self.selector.select(remaining)):
                raise TimeoutError("Actual Native oracle response deadline exceeded")
            chunk = os.read(ready[0][0].fd, min(4096, self.max_bytes - len(self.pending)))
            if not chunk:
                raise ValueError("Actual Native oracle ended before a complete response")
            self.pending.extend(chunk)
