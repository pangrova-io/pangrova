package org.pangrova.streaming.api.cdc;

import org.pangrova.streaming.api.enums.CdcOperation;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Strong-typed immutable domain model wrapping a full CDC event.
 *
 * <p>Uses {@link CdcRow} for explicit column-level typing
 * and adds {@code schemaVersion} for field evolution tracking.</p>
 *
 * <p>Immutable: all fields are final, collections are unmodifiable.</p>
 */
public final class CdcEnvelope {

    private final CdcOperation operation;
    private final String tableName;
    private final CdcRow before;
    private final CdcRow after;
    private final Instant eventTime;
    private final int schemaVersion;
    private final Map<String, String> sourceInfo;

    /**
     * @param operation    CDC operation type (INSERT/UPDATE/DELETE), must not be null
     * @param tableName    fully qualified table name, must not be null
     * @param before       row state before the change (nullable: null for INSERT)
     * @param after        row state after the change (nullable: null for DELETE)
     * @param eventTime    event timestamp, must not be null
     * @param schemaVersion schema version for field evolution tracking (0 if unknown)
     * @param sourceInfo   immutable source metadata from Debezium, must not be null
     */
    public CdcEnvelope(CdcOperation operation, String tableName,
                       CdcRow before, CdcRow after,
                       Instant eventTime, int schemaVersion,
                       Map<String, String> sourceInfo) {
        this.operation = Objects.requireNonNull(operation, "operation must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.before = before;
        this.after = after;
        this.eventTime = Objects.requireNonNull(eventTime, "eventTime must not be null");
        this.schemaVersion = schemaVersion;
        this.sourceInfo = sourceInfo == null ? Map.of() : Map.copyOf(sourceInfo);
    }

    public CdcOperation operation() {
        return operation;
    }

    public String tableName() {
        return tableName;
    }

    public CdcRow before() {
        return before;
    }

    public CdcRow after() {
        return after;
    }

    public Instant eventTime() {
        return eventTime;
    }

    public int schemaVersion() {
        return schemaVersion;
    }

    public Map<String, String> sourceInfo() {
        return sourceInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CdcEnvelope other)) {
            return false;
        }
        return schemaVersion == other.schemaVersion
                && operation == other.operation
                && tableName.equals(other.tableName)
                && hasSameEventData(other);
    }

    private boolean hasSameEventData(CdcEnvelope other) {
        return eventTime.equals(other.eventTime)
                && Objects.equals(before, other.before)
                && Objects.equals(after, other.after)
                && sourceInfo.equals(other.sourceInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, tableName, before, after,
                eventTime, schemaVersion, sourceInfo);
    }

    @Override
    public String toString() {
        return "CdcEnvelope{operation=" + operation
                + ", tableName='" + tableName + '\''
                + ", schemaVersion=" + schemaVersion
                + ", eventTime=" + eventTime
                + '}';
    }
}
