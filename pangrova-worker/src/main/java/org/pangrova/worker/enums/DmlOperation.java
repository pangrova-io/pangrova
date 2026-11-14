package org.pangrova.worker.enums;

/**
 * Semantic DML operation for native write operations.
 *
 * <p>Distinct from the connector-level
 * {@link org.pangrova.connector.api.enums.WriteMode} which models
 * append/upsert/overwrite semantics at the storage layer. This enum
 * models the DML operation intent as seen by the native execution engine.
 */
public enum DmlOperation {
    /** Insert new rows into the target table. */
    INSERT,
    /** Overwrite existing rows (used by UPDATE and full-table MERGE). */
    OVERWRITE,
    /** Delete rows matching a predicate. */
    DELETE;

    /** Decodes the operation selected by the native write executor. */
    public static DmlOperation fromNativeCode(int code) {
        return switch (code) {
            case 0 -> INSERT;
            case 1 -> OVERWRITE;
            case 2 -> DELETE;
            default -> throw new IllegalArgumentException("Unknown native DML operation: " + code);
        };
    }
}
