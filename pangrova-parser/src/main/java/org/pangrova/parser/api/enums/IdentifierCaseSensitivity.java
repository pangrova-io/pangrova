package org.pangrova.parser.api.enums;

/**
 * Case sensitivity policy for unquoted identifiers.
 * Quoted identifiers always preserve their exact case.
 */
public enum IdentifierCaseSensitivity {
    /** Preserve original case (Pangrova/Spark default). */
    PRESERVE,
    /** Fold unquoted identifiers to UPPER case (ANSI standard). */
    UPPER,
    /** Fold unquoted identifiers to lower case (Trino/Presto convention). */
    LOWER
}
