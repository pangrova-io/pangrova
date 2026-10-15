package org.pangrova.spi.connector.metadata.table;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * Immutable ownership of the handle, declaration and statistics selected for one table read.
 *
 * @param handle connector-owned immutable source selector
 * @param declaration schema and declaration evidence captured at binding
 * @param statistics estimates from the selected source, with -1 denoting unknown
 * @param state distinguishes a selected snapshot, an empty view and an unversioned source
 * @param snapshotId positive data snapshot identity, present only for SNAPSHOT
 * @param sourceIdentity connector-owned evidence verified before split enumeration
 */
@Experimental
public record ConnectorReadBinding(ConnectorTableHandle handle,
        ConnectorTableReadMetadata declaration, TableStatistics statistics,
        SnapshotState state, OptionalLong snapshotId, String sourceIdentity) {
    /** An absent data snapshot is distinct from a source that has no snapshot capability. */
    public enum SnapshotState { SNAPSHOT, EMPTY, METADATA, UNVERSIONED }

    public ConnectorReadBinding {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(declaration, "declaration");
        Objects.requireNonNull(statistics, "statistics");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(snapshotId, "snapshotId");
        Objects.requireNonNull(sourceIdentity, "sourceIdentity");
        if ((state == SnapshotState.SNAPSHOT) != snapshotId.isPresent()
                || (snapshotId.isPresent() && snapshotId.getAsLong() <= 0)) {
            throw new IllegalArgumentException("Selected data snapshot must have a positive exact identity");
        }
        if (state == SnapshotState.EMPTY && statistics.rowCount() != 0) {
            throw new IllegalArgumentException("Empty read binding requires proven zero cardinality");
        }
        if (state != SnapshotState.UNVERSIONED && sourceIdentity.isBlank()) {
            throw new IllegalArgumentException("Versioned read binding requires source identity evidence");
        }
        if (statistics.rowCount() < -1 || statistics.dataSize() < -1
                || statistics.sourceWorkUnits() < -1) {
            throw new IllegalArgumentException("Unknown statistics use -1, not another negative value");
        }
    }

    /** A description intentionally omitting catalog properties and connector credentials. */
    @Override
    public String toString() {
        return "ConnectorReadBinding[table=" + declaration.metadata().table()
                + ", state=" + state + ", snapshotId=" + snapshotId + "]";
    }
}
