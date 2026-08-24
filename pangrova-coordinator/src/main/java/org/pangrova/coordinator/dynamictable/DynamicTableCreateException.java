package org.pangrova.coordinator.dynamictable;

/**
 * Thrown when a Dynamic Table CREATE operation fails after partial
 * state has been written. The compensation logic in
 * {@link DynamicTableDdlExecutor} cleans up before this is thrown.
 */
public final class DynamicTableCreateException extends RuntimeException {

    public DynamicTableCreateException(String message, Throwable cause) {
        super(message, cause);
    }
}
