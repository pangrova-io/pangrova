package org.pangrova.parser.api.enums;

/**
 * Identifier quoting style.
 */
public enum IdentifierQuotingStyle {
    /** Only double-quote delimited identifiers ({@code "col"}). */
    DOUBLE_QUOTES,
    /** Only backtick delimited identifiers ({@code `col`}). */
    BACKTICKS,
    /** Both double-quotes and backticks accepted. */
    BOTH
}
