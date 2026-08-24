package org.pangrova.metadata.mv;

import java.time.Instant;
import java.util.Objects;

/**
 * Persistent metadata for a materialized view.
 * Immutable record — all fields are final, no setters.
 *
 * @param mvName          unique name of the materialized view
 * @param baseQuery       the defining SQL query
 * @param state           lifecycle state (CREATED, REFRESHING, ACTIVE, DROPPED)
 * @param refreshStrategy refresh strategy (INCREMENTAL, ON_SCHEDULE, etc.)
 * @param createdAt       creation timestamp
 * @param lastRefreshedAt last successful refresh timestamp (null if never refreshed)
 */
public record MaterializedViewMetadata(
        String mvName,
        String baseQuery,
        MaterializedViewState state,
        String refreshStrategy,
        Instant createdAt,
        Instant lastRefreshedAt) {

    public MaterializedViewMetadata {
        Objects.requireNonNull(mvName, "mvName must not be null");
        Objects.requireNonNull(baseQuery, "baseQuery must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(refreshStrategy, "refreshStrategy must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
    }

    /**
     * Returns a copy with updated state.
     */
    public MaterializedViewMetadata withState(MaterializedViewState newState) {
        return new MaterializedViewMetadata(
                mvName, baseQuery, newState,
                refreshStrategy, createdAt, lastRefreshedAt);
    }

    /**
     * Returns a copy with updated last-refreshed timestamp and ACTIVE state.
     */
    public MaterializedViewMetadata withRefreshed(Instant refreshedAt) {
        return new MaterializedViewMetadata(
                mvName, baseQuery, MaterializedViewState.ACTIVE,
                refreshStrategy, createdAt, refreshedAt);
    }
}
