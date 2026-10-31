package org.pangrova.connector.spi.index.predicate;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Specifies how text matching is performed against an inverted index.
 */
@Experimental
public enum TextMatchMode {

    /** All specified tokens must be present (unordered). */
    MATCH_ALL,

    /** Any specified token is sufficient for a match. */
    MATCH_ANY,

    /** All tokens must appear in consecutive positions (ordered). */
    MATCH_PHRASE
}
