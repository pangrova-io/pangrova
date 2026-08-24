package org.pangrova.spi.execution;

/**
 * Represents the lifecycle state of a query in the Pangrova engine.
 *
 * <p>State transitions follow the order:
 * {@code QUEUED → PLANNING → RUNNING → FINISHING → FINISHED}
 * with {@code FAILED} reachable from any active state.</p>
 */
public enum QueryState {

    /** Query is waiting to be scheduled. */
    QUEUED,

    /** Query plan is being generated. */
    PLANNING,

    /** Query is actively executing. */
    RUNNING,

    /** Query execution is completing; results are being finalized. */
    FINISHING,

    /** Query completed successfully. */
    FINISHED,

    /** Query terminated due to an error. */
    FAILED
}
