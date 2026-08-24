package org.pangrova.planner.mv;

import org.pangrova.parser.ast.RefreshStrategy;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * Holds metadata for a registered materialized view.
 *
 * @param name                 MV name
 * @param query                defining SQL query text
 * @param refreshStrategy      refresh strategy
 * @param enableQueryRewrite   whether this MV participates in query rewrite
 * @param lastRefreshSnapshot  timestamp of last refresh
 * @param createdAt            creation timestamp
 * @param baseTableNames       fully-qualified table names referenced by the defining query
 *                             (empty set when not available — disables structural matching)
 * @param outputColumnNames    lowercase column names produced by the defining query
 *                             (empty set when not available — disables structural matching)
 * @param groupByColumnNames   lowercase GROUP BY column names in the defining query
 *                             (empty set when the MV has no aggregation)
 */
public record MaterializedViewDefinition(
        String name,
        String query,
        RefreshStrategy refreshStrategy,
        boolean enableQueryRewrite,
        Optional<Instant> lastRefreshSnapshot,
        Instant createdAt,
        Set<String> baseTableNames,
        Set<String> outputColumnNames,
        Set<String> groupByColumnNames) {

    public MaterializedViewDefinition {
        baseTableNames = baseTableNames != null ? Set.copyOf(baseTableNames) : Set.of();
        outputColumnNames = outputColumnNames != null ? Set.copyOf(outputColumnNames) : Set.of();
        groupByColumnNames = groupByColumnNames != null ? Set.copyOf(groupByColumnNames) : Set.of();
    }

    /**
     * Constructor without structural metadata.
     */
    public MaterializedViewDefinition(
            String name, String query, RefreshStrategy refreshStrategy,
            boolean enableQueryRewrite, Optional<Instant> lastRefreshSnapshot,
            Instant createdAt) {
        this(name, query, refreshStrategy, enableQueryRewrite,
                lastRefreshSnapshot, createdAt, Set.of(), Set.of(), Set.of());
    }

    /**
     * Returns a copy with an updated last-refresh snapshot.
     */
    public MaterializedViewDefinition withRefreshSnapshot(Instant snapshot) {
        return new MaterializedViewDefinition(
                name, query, refreshStrategy, enableQueryRewrite,
                Optional.of(snapshot), createdAt, baseTableNames, outputColumnNames,
                groupByColumnNames);
    }

    /**
     * Returns a copy with the enableQueryRewrite flag toggled.
     */
    public MaterializedViewDefinition withEnableQueryRewrite(boolean enabled) {
        return new MaterializedViewDefinition(
                name, query, refreshStrategy, enabled,
                lastRefreshSnapshot, createdAt, baseTableNames, outputColumnNames,
                groupByColumnNames);
    }

    /**
     * Returns true when structural metadata is available for subsumption matching.
     */
    public boolean hasStructuralMetadata() {
        return !baseTableNames.isEmpty();
    }
}
