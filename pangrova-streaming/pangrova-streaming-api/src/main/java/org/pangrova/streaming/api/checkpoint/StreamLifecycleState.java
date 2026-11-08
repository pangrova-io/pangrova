package org.pangrova.streaming.api.checkpoint;

/**
 * Lifecycle states for an ingestion stream.
 *
 * <p>State machine transitions:</p>
 * <pre>
 *   CREATING → RUNNING → SUSPENDED → RUNNING (resume)
 *                      → FAILED    → RUNNING (resume)
 *                      → DROPPING  → DROPPED
 * </pre>
 */
public enum StreamLifecycleState {
    CREATING,
    RUNNING,
    SUSPENDED,
    FAILED,
    DROPPING,
    DROPPED
}
