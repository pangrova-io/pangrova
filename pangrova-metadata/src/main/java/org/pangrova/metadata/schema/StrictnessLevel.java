package org.pangrova.metadata.schema;

/**
 * Controls how strictly schema evolution changes are validated.
 *
 * <ul>
 *   <li>{@link #LENIENT} — allows all changes including drops and unsafe type changes (warnings only)</li>
 *   <li>{@link #STANDARD} — rejects unsafe type changes and nullable-to-non-nullable, allows drops</li>
 *   <li>{@link #STRICT} — additionally rejects column drops and renames (append-only evolution)</li>
 * </ul>
 */
public enum StrictnessLevel {

    /**
     * Lenient: all changes produce warnings but never errors.
     * Suitable for development environments where schema flexibility is prioritized.
     */
    LENIENT,

    /**
     * Standard: rejects unsafe type narrowing and nullable-to-non-nullable changes.
     * Allows column additions, drops, and renames. Default level.
     */
    STANDARD,

    /**
     * Strict: append-only evolution. Rejects column drops and renames in addition
     * to all STANDARD restrictions. Suitable for production tables with downstream consumers.
     */
    STRICT
}
