"""Native RSS peaks for the current executable, independent of the test parent."""

from __future__ import annotations

import resource
import sys
from pathlib import Path


def peak_rss_bytes() -> int:
    """Return this executable's peak resident memory in bytes.

    Linux getrusage can retain the pre-exec address space's high-water mark;
    VmHWM belongs to the current address space. Darwin's getrusage reads the
    current Mach task's resident_size_max, and exec replaces that task.
    """
    if sys.platform.startswith("linux"):
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
        message = "Current-process VmHWM is unavailable"
        raise RuntimeError(message)
    if sys.platform == "darwin":
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    message = f"RSS qualification does not support {sys.platform}"
    raise RuntimeError(message)
