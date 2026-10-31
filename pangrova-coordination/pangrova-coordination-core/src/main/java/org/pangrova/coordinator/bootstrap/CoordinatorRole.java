package org.pangrova.coordinator.bootstrap;

/**
 * Deployment role for a coordinator instance.
 *
 * <ul>
 *   <li>{@link #QUERY} — serves queries only: HTTP server, QueryManager, MetadataCache.
 *       Does not participate in leader election or run background maintenance.</li>
 *   <li>{@link #MAINTENANCE} — runs background maintenance only: leader election,
 *       indexing, streaming, dynamic table refresh. Does not serve client queries.</li>
 *   <li>{@link #COMBINED} — both query serving and maintenance (default, backward compatible).</li>
 * </ul>
 */
public enum CoordinatorRole {

    QUERY,
    MAINTENANCE,
    COMBINED;

    /** Returns true if this role serves client queries (HTTP + QueryManager + MetadataCache). */
    public boolean isQuery() {
        return this == QUERY || this == COMBINED;
    }

    /** Returns true if this role runs background maintenance (leader election + indexing + streaming + DT refresh). */
    public boolean isMaintenance() {
        return this == MAINTENANCE || this == COMBINED;
    }
}
