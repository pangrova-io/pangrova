package org.pangrova.materialization.domain;

import java.util.UUID;

/**
 * Identity of a query that holds a reader pin on a generation.
 *
 * <p>Query identifiers are coordinator-issued strings, so the pin identity must
 * preserve the exact value the query uses for renewal and release.
 *
 * @param value query identifier
 */
public record QueryId(String value) {

    private static final int MAX_LENGTH = 128;

    public QueryId {
        value = Require.text(value, "query id");
        if (value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("query id exceeds " + MAX_LENGTH + " characters");
        }
    }

    /**
     * Creates a fresh random query identifier.
     *
     * @return new identifier
     */
    public static QueryId random() {
        return new QueryId(UUID.randomUUID().toString());
    }
}
