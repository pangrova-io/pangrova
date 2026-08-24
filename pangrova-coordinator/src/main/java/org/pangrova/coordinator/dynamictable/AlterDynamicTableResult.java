package org.pangrova.coordinator.dynamictable;

import java.util.Objects;

/**
 * Immutable result of an ALTER DYNAMIC TABLE operation.
 *
 * @param dtName  altered Dynamic Table name
 * @param action  action that was performed (e.g., "SUSPEND", "RESUME")
 * @param message human-readable status message
 */
public record AlterDynamicTableResult(
        String dtName,
        String action,
        String message
) {

    public AlterDynamicTableResult {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(message, "message must not be null");
    }
}
