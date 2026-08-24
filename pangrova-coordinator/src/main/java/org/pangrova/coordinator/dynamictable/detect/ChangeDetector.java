package org.pangrova.coordinator.dynamictable.detect;

import org.pangrova.metadata.dynamictable.DynamicTableState;

/**
 * Detects changes in a source table since the last committed checkpoint.
 *
 * <p>Implementations choose the detection strategy based on the source
 * connector's capabilities:</p>
 * <ul>
 *   <li>{@link SnapshotChangeDetector} — snapshot-diff based (Iceberg / Paimon)</li>
 *   <li>CdcChangeDetector — CDC stream based (Debezium)</li>
 * </ul>
 *
 * <p>Implementations must be thread-safe; the coordinator may invoke
 * {@link #detect} concurrently for different Dynamic Tables.</p>
 */
public interface ChangeDetector {

    /**
     * Detect changes in the source table since the last checkpoint
     * recorded in {@code state}.
     *
     * @param state current Dynamic Table state including checkpoint
     * @return detection result — empty, incremental, CDC batch, or full-refresh-required
     */
    ChangeResult detect(DynamicTableState state);
}
