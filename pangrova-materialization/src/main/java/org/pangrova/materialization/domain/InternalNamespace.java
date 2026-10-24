package org.pangrova.materialization.domain;

import java.util.regex.Pattern;

/**
 * Reserved identifiers of the internal materialization namespace.
 *
 * <p>Every entry point that resolves, authorizes, parses, or creates an object
 * must use these predicates. Scattering string comparisons across modules is
 * forbidden because a single missed entry point would expose physical
 * generations to user access.
 */
public final class InternalNamespace {

    /** Internal PostgreSQL schema holding physical generations. */
    public static final String SCHEMA_NAME = "__pangrova_materializations";

    /** Prefix of every physical candidate table. */
    public static final String CANDIDATE_PREFIX = "__pg_mat_";

    /** Legacy Dynamic Table prefix, reserved forever to prevent alias re-use. */
    public static final String LEGACY_CANDIDATE_PREFIX = "__pg_dt_";

    private static final Pattern CANDIDATE_TABLE = Pattern.compile("^__pg_mat_[0-9a-f]{32}$");

    private InternalNamespace() {
    }

    /**
     * Returns whether a schema name is the internal materialization schema.
     *
     * @param schema schema name
     * @return true for the reserved schema
     */
    public static boolean isReservedSchema(String schema) {
        return SCHEMA_NAME.equals(schema);
    }

    /**
     * Returns whether a table name uses a reserved materialization prefix.
     *
     * @param table table name
     * @return true for reserved table names
     */
    public static boolean isReservedTable(String table) {
        if (table == null) {
            return false;
        }
        String lower = table.toLowerCase(java.util.Locale.ROOT);
        return lower.startsWith(CANDIDATE_PREFIX) || lower.startsWith(LEGACY_CANDIDATE_PREFIX);
    }

    /**
     * Returns whether a table name is a well-formed physical candidate name.
     *
     * @param table table name
     * @return true for {@code __pg_mat_<32 lowercase hex>}
     */
    public static boolean isCandidateTable(String table) {
        return table != null && CANDIDATE_TABLE.matcher(table).matches();
    }

    /**
     * Creates a fresh candidate table name from a random identifier.
     *
     * @param randomHex 32 lowercase hexadecimal characters
     * @return candidate table name
     */
    public static String candidateTableName(String randomHex) {
        if (randomHex == null || !randomHex.matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException("randomHex must be 32 lowercase hex characters");
        }
        return CANDIDATE_PREFIX + randomHex;
    }
}
