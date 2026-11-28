#!/usr/bin/env python3
"""Sample macOS role processes launched by sql_process_comparison.py until its forks finish."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
import ctypes
import os
from pathlib import Path
import re
import subprocess
import sys
import time


# libproc's public proc_taskinfo layout from the macOS SDK <sys/proc_info.h>.
class TaskInfo(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in (
        "virtual", "resident", "user", "system", "thread_user", "thread_system"
    )] + [(name, ctypes.c_int32) for name in (
        "policy", "faults", "pageins", "cow", "sent", "received", "mach_syscalls",
        "unix_syscalls", "context_switches", "threads", "running", "priority"
    )]


class Descriptor(ctypes.Structure):
    _fields_ = [("index", ctypes.c_int32), ("kind", ctypes.c_uint32)]


def observe(lib, mach_timebase, jstat, target):
    role, pid = target
    task = TaskInfo()
    if lib.proc_pidinfo(pid, 4, 0, ctypes.byref(task), ctypes.sizeof(task)) != ctypes.sizeof(task):
        return None
    count = lib.proc_pidinfo(pid, 1, 0, None, 0)
    if count <= 0:
        return None
    # The descriptor table may grow between the size query and the actual observation.
    descriptors = (Descriptor * (count // ctypes.sizeof(Descriptor) + 64))()
    size = lib.proc_pidinfo(pid, 1, 0, descriptors, ctypes.sizeof(descriptors))
    if size <= 0 or size >= ctypes.sizeof(descriptors):
        return None
    total = size // ctypes.sizeof(Descriptor)
    sockets = sum(descriptors[i].kind == 2 for i in range(total))
    heap = ""
    try:
        result = subprocess.run([jstat, "-gc", str(pid)], text=True, capture_output=True,
                                timeout=8, check=True).stdout.splitlines()
        values = dict(zip(result[0].split(), result[1].split()))
        heap = round(sum(float(values[k]) for k in ("S0U", "S1U", "EU", "OU")) * 1024)
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, IndexError):
        pass  # A disappearing JVM can still yield a valid OS sample; leave heap unobserved.
    numerator, denominator = mach_timebase
    cpu_time_ns = (task.user + task.system) * numerator // denominator
    return role, pid, task.resident, heap, task.threads, total, sockets, cpu_time_ns


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Fork files named <fork>.log and <fork>.csv")
    args = parser.parse_args()
    if sys.platform != "darwin" or not os.environ.get("JAVA_HOME"):
        parser.error("Requires macOS and JAVA_HOME pointing to the JDK 17 used by the Workers")
    lib = ctypes.CDLL("/usr/lib/libproc.dylib")
    mach_timebase = (ctypes.c_uint32 * 2)()
    if ctypes.CDLL(None).mach_timebase_info(ctypes.byref(mach_timebase)) != 0 or mach_timebase[1] == 0:
        raise RuntimeError("Cannot read the Mach CPU timebase")
    jstat = Path(os.environ["JAVA_HOME"]) / "bin/jstat"
    root = args.directory
    with (root / "role-samples.csv").open("x") as out, ThreadPoolExecutor(max_workers=3) as executor:
        csvout = csv.writer(out)
        csvout.writerow(["fork", "timestamp_ns", "accepted_queries", "role", "pid", "rss_bytes",
                         "sampled_heap_used_bytes", "thread_count", "open_descriptors", "socket_descriptors",
                         "cpu_time_ns"])
        while not (root / "sampling-complete").exists():
            for log in sorted(root.glob("*.log")):
                matches = re.findall(r"role=(coordinator|local-worker-[12]) pid=(\d+)", log.read_text())
                query_csv = log.with_suffix(".csv")
                samples = max(0, len(query_csv.read_text().splitlines()) - 1) if query_csv.exists() else 0
                targets = [(role, int(pid)) for role, pid in matches]
                for record in executor.map(lambda t: observe(lib, mach_timebase, jstat, t), targets):
                    if record is not None:
                        csvout.writerow([log.stem, time.time_ns(), samples, *record])
            out.flush()
            time.sleep(3)


if __name__ == "__main__":
    main()
