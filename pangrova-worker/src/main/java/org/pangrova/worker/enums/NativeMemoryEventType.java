package org.pangrova.worker.enums;

/** Kind of memory event reported by the native runtime. */
public enum NativeMemoryEventType {
    /** Periodic heartbeat stat report. */
    STAT,
    /** Native triggered a spill for this tenant. */
    SPILL,
    /** Native shrunk allocation for this tenant. */
    SHRINK,
    /** Native encountered OOM for this tenant. */
    OOM
}
