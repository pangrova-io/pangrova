package org.pangrova.parser.api.enums;

/**
 * Keyword classification.
 */
public enum KeywordCategory {
    /**
     * The keyword is reserved: it cannot be used as an unquoted identifier.
     * Must be quoted (double-quotes or backticks, per dialect) to use as an
     * identifier.
     */
    RESERVED,

    /**
     * The keyword is non-reserved: it is recognized as a keyword in certain
     * syntactic positions but can also be used as an unquoted identifier
     * without quoting.
     */
    NON_RESERVED
}
