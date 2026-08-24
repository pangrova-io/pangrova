package org.pangrova.metadata.dynamictable;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Immutable state record for a Dynamic Table's incremental refresh lifecycle.
 *
 * <p>Persisted by {@link DynamicTableStateStore}. All mutation methods return
 * a new instance — the original is never modified.</p>
 *
 * @param dtName                 unique Dynamic Table name (required)
 * @param tenantId               owning tenant identifier (required)
 * @param state                  current lifecycle state (required)
 * @param effectiveRefreshMode   actual refresh mode in use (required)
 * @param targetLag              declared data freshness SLA (required)
 * @param checkpoint             opaque checkpoint — snapshot ID or CDC offset (nullable)
 * @param checkpointType         "SNAPSHOT" or "CDC_OFFSET" (nullable)
 * @param sourceSchemaVersion    schema version recorded at creation time
 * @param sourceSchemaSnapshot   JSON-serialized column definitions (nullable)
 * @param creatorIdentity        definer identity for refresh execution (nullable)
 * @param coordinatorId          owning coordinator for HA fencing (nullable)
 * @param epoch                  HA epoch number for fencing
 * @param lastRefreshTime        timestamp of last successful refresh (nullable)
 * @param totalRefreshCount      cumulative successful refresh count
 * @param consecutiveFailures    consecutive failure count (reset on success)
 * @param createdAt              creation timestamp (required)
 * @param updatedAt              last update timestamp (required)
 */
public record DynamicTableState(
        String dtName,
        String tenantId,
        DynamicTableLifecycleState state,
        DynamicTableRefreshMode effectiveRefreshMode,
        Duration targetLag,
        String checkpoint,
        String checkpointType,
        int sourceSchemaVersion,
        String sourceSchemaSnapshot,
        String creatorIdentity,
        String coordinatorId,
        long epoch,
        Instant lastRefreshTime,
        long totalRefreshCount,
        int consecutiveFailures,
        Instant createdAt,
        Instant updatedAt
) {

    public DynamicTableState {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(effectiveRefreshMode, "effectiveRefreshMode must not be null");
        Objects.requireNonNull(targetLag, "targetLag must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }

    /**
     * Return a new state with the given lifecycle state and updated timestamp.
     */
    public DynamicTableState withState(DynamicTableLifecycleState newState) {
        return toBuilder().state(newState).updatedAt(Instant.now()).build();
    }

    /**
     * Return a new state with an updated checkpoint, reset consecutive failures
     * to zero, increment total refresh count, and update last refresh time.
     */
    public DynamicTableState withCheckpoint(String newCheckpoint, String newCheckpointType) {
        Instant now = Instant.now();
        return toBuilder()
                .checkpoint(newCheckpoint).checkpointType(newCheckpointType)
                .lastRefreshTime(now).totalRefreshCount(totalRefreshCount + 1)
                .consecutiveFailures(0).updatedAt(now).build();
    }

    /**
     * Return a new state with consecutive failures incremented by one.
     */
    public DynamicTableState withFailure() {
        return toBuilder()
                .consecutiveFailures(consecutiveFailures + 1)
                .updatedAt(Instant.now()).build();
    }

    /**
     * Return a new state with the given effective refresh mode.
     */
    public DynamicTableState withEffectiveRefreshMode(DynamicTableRefreshMode newMode) {
        return toBuilder()
                .effectiveRefreshMode(newMode)
                .updatedAt(Instant.now()).build();
    }

    /** Create a builder pre-populated with all fields from this state. */
    public Builder toBuilder() {
        return new Builder()
                .dtName(dtName).tenantId(tenantId).state(state)
                .effectiveRefreshMode(effectiveRefreshMode).targetLag(targetLag)
                .checkpoint(checkpoint).checkpointType(checkpointType)
                .sourceSchemaVersion(sourceSchemaVersion)
                .sourceSchemaSnapshot(sourceSchemaSnapshot)
                .creatorIdentity(creatorIdentity).coordinatorId(coordinatorId)
                .epoch(epoch).lastRefreshTime(lastRefreshTime)
                .totalRefreshCount(totalRefreshCount)
                .consecutiveFailures(consecutiveFailures)
                .createdAt(createdAt).updatedAt(updatedAt);
    }

    /** Create an empty builder. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Mutable builder for {@link DynamicTableState}.
     * Reduces the 17-param constructor duplication across mutation sites.
     */
    public static final class Builder {
        private String dtName;
        private String tenantId;
        private DynamicTableLifecycleState state;
        private DynamicTableRefreshMode effectiveRefreshMode;
        private Duration targetLag;
        private String checkpoint;
        private String checkpointType;
        private int sourceSchemaVersion;
        private String sourceSchemaSnapshot;
        private String creatorIdentity;
        private String coordinatorId;
        private long epoch;
        private Instant lastRefreshTime;
        private long totalRefreshCount;
        private int consecutiveFailures;
        private Instant createdAt;
        private Instant updatedAt;

        private Builder() {}

        public Builder dtName(String v) {
            this.dtName = v;
            return this;
        }
        public Builder tenantId(String v) {
            this.tenantId = v;
            return this;
        }
        public Builder state(DynamicTableLifecycleState v) {
            this.state = v;
            return this;
        }
        public Builder effectiveRefreshMode(DynamicTableRefreshMode v) {
            this.effectiveRefreshMode = v;
            return this;
        }
        public Builder targetLag(Duration v) {
            this.targetLag = v;
            return this;
        }
        public Builder checkpoint(String v) {
            this.checkpoint = v;
            return this;
        }
        public Builder checkpointType(String v) {
            this.checkpointType = v;
            return this;
        }
        public Builder sourceSchemaVersion(int v) {
            this.sourceSchemaVersion = v;
            return this;
        }
        public Builder sourceSchemaSnapshot(String v) {
            this.sourceSchemaSnapshot = v;
            return this;
        }
        public Builder creatorIdentity(String v) {
            this.creatorIdentity = v;
            return this;
        }
        public Builder coordinatorId(String v) {
            this.coordinatorId = v;
            return this;
        }
        public Builder epoch(long v) {
            this.epoch = v;
            return this;
        }
        public Builder lastRefreshTime(Instant v) {
            this.lastRefreshTime = v;
            return this;
        }
        public Builder totalRefreshCount(long v) {
            this.totalRefreshCount = v;
            return this;
        }
        public Builder consecutiveFailures(int v) {
            this.consecutiveFailures = v;
            return this;
        }
        public Builder createdAt(Instant v) {
            this.createdAt = v;
            return this;
        }
        public Builder updatedAt(Instant v) {
            this.updatedAt = v;
            return this;
        }

        /** Builds an immutable {@link DynamicTableState} from the current builder values. */
        public DynamicTableState build() {
            return new DynamicTableState(
                    dtName, tenantId, state, effectiveRefreshMode, targetLag,
                    checkpoint, checkpointType, sourceSchemaVersion, sourceSchemaSnapshot,
                    creatorIdentity, coordinatorId, epoch, lastRefreshTime,
                    totalRefreshCount, consecutiveFailures, createdAt, updatedAt);
        }
    }

}
