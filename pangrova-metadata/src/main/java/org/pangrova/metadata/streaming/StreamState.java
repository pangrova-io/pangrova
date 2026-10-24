package org.pangrova.metadata.streaming;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable state record for a CDC ingestion stream.
 *
 * <p>Persisted by {@link StreamStateStore}. All mutation methods return
 * a new instance — the original is never modified. Fields match the
 * {@code ingestion_stream_state} database table columns.</p>
 *
 * @param streamName          unique stream name (primary key)
 * @param tenantId            owning tenant identifier
 * @param checkpointId        immutable identity for this creation's checkpoints and default consumer group
 * @param lifecycleState      current lifecycle state
 * @param targetTable         fully qualified target lakehouse table reference
 * @param sourceProperties    Kafka source properties (bootstrap.servers, topic, group.id, etc.)
 * @param sourceFormat        CDC format identifier (debezium-json, canal-json, maxwell-json)
 * @param streamProperties    stream-level WITH properties (batch_size, flush_interval, etc.)
 * @param creatorIdentity     identity of the user who created the stream
 * @param coordinatorId       owning coordinator for HA fencing
 * @param epoch               HA epoch number for optimistic concurrency control
 * @param schemaVersion       schema version recorded at creation time
 * @param errorMessage        error details when in FAILED state
 * @param recordsConsumed     cumulative records consumed from source
 * @param recordsWritten      cumulative records written to target
 * @param recordsDeadLettered cumulative records routed to dead-letter
 * @param lastOffsetCommitTime timestamp of last successful offset commit
 * @param createdAt           creation timestamp
 * @param updatedAt           last update timestamp
 */
public record StreamState(
        String streamName,
        String tenantId,
        String checkpointId,
        StreamLifecycleState lifecycleState,
        String targetTable,
        Map<String, String> sourceProperties,
        String sourceFormat,
        Map<String, String> streamProperties,
        String creatorIdentity,
        String coordinatorId,
        long epoch,
        int schemaVersion,
        String errorMessage,
        long recordsConsumed,
        long recordsWritten,
        long recordsDeadLettered,
        Instant lastOffsetCommitTime,
        Instant createdAt,
        Instant updatedAt
) {

    public StreamState {
        Objects.requireNonNull(streamName, "streamName must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        if (checkpointId == null || checkpointId.isBlank()) {
            throw new IllegalArgumentException("checkpointId must not be blank");
        }
        Objects.requireNonNull(lifecycleState, "lifecycleState must not be null");
        Objects.requireNonNull(targetTable, "targetTable must not be null");
        Objects.requireNonNull(sourceProperties, "sourceProperties must not be null");
        Objects.requireNonNull(sourceFormat, "sourceFormat must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        sourceProperties = Map.copyOf(sourceProperties);
        streamProperties = streamProperties != null ? Map.copyOf(streamProperties) : Map.of();
    }

    /** Return a new state with the given lifecycle state and updated timestamp. */
    public StreamState withLifecycleState(StreamLifecycleState newState) {
        return toBuilder().lifecycleState(newState).updatedAt(Instant.now()).build();
    }

    /** Return a new state with the given error message and FAILED lifecycle state. */
    public StreamState withError(String message) {
        return toBuilder()
                .lifecycleState(StreamLifecycleState.FAILED)
                .errorMessage(message)
                .updatedAt(Instant.now())
                .build();
    }

    /** Return a new state with incremented epoch and updated coordinator. */
    public StreamState withReacquisition(String newCoordinatorId) {
        return toBuilder()
                .coordinatorId(newCoordinatorId)
                .epoch(epoch + 1)
                .updatedAt(Instant.now())
                .build();
    }

    /** Create a builder pre-populated with all fields from this state. */
    public Builder toBuilder() {
        return new Builder()
                .streamName(streamName).tenantId(tenantId).checkpointId(checkpointId)
                .lifecycleState(lifecycleState).targetTable(targetTable)
                .sourceProperties(sourceProperties).sourceFormat(sourceFormat)
                .streamProperties(streamProperties).creatorIdentity(creatorIdentity)
                .coordinatorId(coordinatorId).epoch(epoch)
                .schemaVersion(schemaVersion).errorMessage(errorMessage)
                .recordsConsumed(recordsConsumed).recordsWritten(recordsWritten)
                .recordsDeadLettered(recordsDeadLettered)
                .lastOffsetCommitTime(lastOffsetCommitTime)
                .createdAt(createdAt).updatedAt(updatedAt);
    }

    /** Create an empty builder. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Mutable builder for {@link StreamState}.
     * Reduces the 19-param constructor duplication across mutation sites.
     */
    public static final class Builder {
        private String streamName;
        private String tenantId;
        private String checkpointId = "stream:" + UUID.randomUUID();
        private StreamLifecycleState lifecycleState;
        private String targetTable;
        private Map<String, String> sourceProperties;
        private String sourceFormat;
        private Map<String, String> streamProperties;
        private String creatorIdentity;
        private String coordinatorId;
        private long epoch;
        private int schemaVersion;
        private String errorMessage;
        private long recordsConsumed;
        private long recordsWritten;
        private long recordsDeadLettered;
        private Instant lastOffsetCommitTime;
        private Instant createdAt;
        private Instant updatedAt;

        private Builder() {}

        public Builder streamName(String v) {
            this.streamName = v;
            return this;
        }
        public Builder tenantId(String v) {
            this.tenantId = v;
            return this;
        }
        /** Restores the immutable checkpoint identity of an existing stream. */
        public Builder checkpointId(String v) {
            this.checkpointId = v;
            return this;
        }
        public Builder lifecycleState(StreamLifecycleState v) {
            this.lifecycleState = v;
            return this;
        }
        public Builder targetTable(String v) {
            this.targetTable = v;
            return this;
        }
        public Builder sourceProperties(Map<String, String> v) {
            this.sourceProperties = v;
            return this;
        }
        public Builder sourceFormat(String v) {
            this.sourceFormat = v;
            return this;
        }
        public Builder streamProperties(Map<String, String> v) {
            this.streamProperties = v;
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
        public Builder schemaVersion(int v) {
            this.schemaVersion = v;
            return this;
        }
        public Builder errorMessage(String v) {
            this.errorMessage = v;
            return this;
        }
        public Builder recordsConsumed(long v) {
            this.recordsConsumed = v;
            return this;
        }
        public Builder recordsWritten(long v) {
            this.recordsWritten = v;
            return this;
        }
        public Builder recordsDeadLettered(long v) {
            this.recordsDeadLettered = v;
            return this;
        }
        public Builder lastOffsetCommitTime(Instant v) {
            this.lastOffsetCommitTime = v;
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

        /** Builds an immutable {@link StreamState} from the current builder values. */
        public StreamState build() {
            return new StreamState(
                    streamName, tenantId, checkpointId, lifecycleState, targetTable,
                    sourceProperties, sourceFormat, streamProperties,
                    creatorIdentity, coordinatorId, epoch, schemaVersion,
                    errorMessage, recordsConsumed, recordsWritten,
                    recordsDeadLettered, lastOffsetCommitTime,
                    createdAt, updatedAt);
        }
    }
}
