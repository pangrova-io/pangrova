package org.pangrova.indexing.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Represents the lifecycle state of an index.
 *
 * <p>State transitions follow a strict state machine:
 * <ul>
 *   <li>{@code BUILDING → BUILDING | READY | STALE | DROPPED}</li>
 *   <li>{@code READY → BUILDING | STALE | DROPPED}</li>
 *   <li>{@code STALE → BUILDING | DROPPED}</li>
 *   <li>{@code DROPPED → (terminal, no transitions allowed)}</li>
 * </ul>
 */
@Experimental
public enum IndexState {

    /** Index is currently being built or rebuilt. */
    BUILDING,

    /** Index is fully built and available for query evaluation. */
    READY,

    /** Index is partially valid due to data changes or build failures. */
    STALE,

    /** Index has been dropped and is pending garbage collection. */
    DROPPED
}
