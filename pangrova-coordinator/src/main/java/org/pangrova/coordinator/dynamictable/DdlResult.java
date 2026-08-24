package org.pangrova.coordinator.dynamictable;

/**
 * Result of a Dynamic Table DDL operation.
 *
 * <p>{@link Success} carries a human-readable message.
 * {@link Rejected} carries the rejection reason.</p>
 */
public sealed interface DdlResult {

    /** DDL operation completed successfully. */
    record Success(String message) implements DdlResult {}

    /** DDL operation was rejected (validation failure, conflict, etc.). */
    record Rejected(String reason) implements DdlResult {}
}
