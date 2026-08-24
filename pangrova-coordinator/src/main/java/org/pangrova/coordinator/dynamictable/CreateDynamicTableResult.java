package org.pangrova.coordinator.dynamictable;

import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;

import java.util.Objects;

/**
 * Immutable result of a CREATE DYNAMIC TABLE operation.
 *
 * @param dtName              created Dynamic Table name
 * @param effectiveRefreshMode resolved refresh mode (AUTO may resolve to CDC or SNAPSHOT)
 * @param message             human-readable status message
 */
public record CreateDynamicTableResult(
        String dtName,
        DynamicTableRefreshMode effectiveRefreshMode,
        String message
) {

    public CreateDynamicTableResult {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(effectiveRefreshMode, "effectiveRefreshMode must not be null");
        Objects.requireNonNull(message, "message must not be null");
    }
}
