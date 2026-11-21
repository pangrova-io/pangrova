package org.pangrova.worker.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Identifies a memory domain tracked by the {@link org.pangrova.worker.resource.memory.MemoryBudgetEnforcer}.
 *
 * <p>Pangrova's worker process manages memory across three domains that must
 * share a single physical budget: Java heap, native off-heap, and
 * additional off-heap pools (off-heap buffers, shuffle buffers, etc.).</p>
 */
@Experimental(reason = "G-RES-07: unified memory budget enforcement SPI")
public enum MemoryDomain {

    /** Java heap memory managed by the JVM garbage collector. */
    JAVA_HEAP,

    /** Native off-heap memory managed by the native arbitrator. */
    NATIVE,

    /** Additional off-heap pools (off-heap buffers, shuffle buffers, etc.). */
    OFF_HEAP
}
