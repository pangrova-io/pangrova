package org.pangrova.planner.core.cache;

import java.util.Objects;
import java.util.Set;

/**
 * Immutable metadata stored alongside each cache entry for precise invalidation.
 *
 * <p>Enables metadata-indexed invalidation by table set, user, or policy version,
 * replacing fragile {@code key.contains(...)} string matching.</p>
 *
 * @param tables        unmodifiable set of table names referenced by the cached query (lowercase)
 * @param user          the principal who executed the query
 * @param policyVersion the security policy version at cache time
 */
public record CacheEntryMetadata(Set<String> tables, String user, String policyVersion) {

    public CacheEntryMetadata {
        Objects.requireNonNull(tables, "tables must not be null");
        Objects.requireNonNull(user, "user must not be null");
        Objects.requireNonNull(policyVersion, "policyVersion must not be null");
        tables = Set.copyOf(tables);
    }

    /**
     * Returns true if this entry references the given table (case-insensitive).
     */
    public boolean referencesTable(String tableName) {
        return tables.contains(tableName.toLowerCase());
    }
}
