package org.pangrova.materialization.extension;

/**
 * Local argument checks for extension DTOs.
 */
final class RequireExtension {

    private RequireExtension() {
    }

    static <T> T notNull(T value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " must not be null");
        }
        return value;
    }
}
