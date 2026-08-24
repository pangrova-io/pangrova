package org.pangrova.connector.iceberg.write;

import java.util.List;

import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;

/**
 * Single optimistic-concurrency validation step applied at the connector-side atomic commit
 * chokepoint ({@code IcebergMetadata.finishDistributedWrite}). This is the one place engine-side
 * catalog commits happen, so write-path conflict validation cannot be bypassed.
 *
 * <p>Per operation it asks Iceberg's commit builders to validate against the snapshot the write was
 * planned from ({@code baseSnapshotId}), so a concurrent OVERWRITE / copy-on-write DELETE/UPDATE/MERGE
 * is rejected (a retryable serialization error) instead of silently losing rows:
 * <ul>
 *   <li>OVERWRITE / COW-DELETE → {@code ReplacePartitions.validateFromSnapshot(base)
 *       .validateNoConflictingData().validateNoConflictingDeletes()}</li>
 *   <li>merge-on-read RowDelta → {@code RowDelta.validateFromSnapshot(base)} (data-files-exist /
 *       no-conflicting-delete-files are added when the real MOR path is wired)</li>
 * </ul>
 *
 * <p>Validation is <b>unconditional</b> whenever the write was planned from a real base snapshot
 * ({@code baseSnapshotId >= 0}); a write against an empty table / INSERT append carries no base and is
 * not validated (appends never conflict). On a real conflict Iceberg throws {@code ValidationException}
 * at {@code commit()}, which propagates and fails the query instead of silently overwriting — the S0
 * correctness guarantee (G-DML-01 / G-CONN-01 / F-ICEBERG-01/04). Bounded rebase-retry on conflict is
 * a follow-up refinement (F-CHAIN-WRITE-02).
 */
public final class ConflictValidatingCommit {

    private ConflictValidatingCommit() {
    }

    /**
     * Applies OVERWRITE / COW-DELETE conflict validation to a {@link ReplacePartitions} builder.
     *
     * @param update         the in-progress ReplacePartitions commit
     * @param baseSnapshotId snapshot the write was planned from, or a negative sentinel when none
     * @return the same builder, with validations attached when a base snapshot is present
     */
    public static ReplacePartitions validate(ReplacePartitions update, long baseSnapshotId) {
        if (baseSnapshotId < 0) {
            return update;
        }
        return update
                .validateFromSnapshot(baseSnapshotId)
                .validateNoConflictingData()
                .validateNoConflictingDeletes();
    }

    /**
     * Applies merge-on-read conflict validation to a {@link RowDelta} builder (DELETE / MERGE) without
     * a referenced-data-file manifest. Equivalent to {@link #validate(RowDelta, long, Iterable)} with
     * an empty manifest: only the snapshot-lineage check is attached.
     *
     * @param update         the in-progress RowDelta commit
     * @param baseSnapshotId snapshot the write was planned from, or a negative sentinel when none
     * @return the same builder, with validations attached when a base snapshot is present
     */
    public static RowDelta validate(RowDelta update, long baseSnapshotId) {
        return validate(update, baseSnapshotId, List.of());
    }

    /**
     * Applies merge-on-read conflict validation to a {@link RowDelta} builder (DELETE / MERGE).
     *
     * <p>Beyond the snapshot-lineage check ({@code validateFromSnapshot}), when the commit carries the
     * set of data files its position deletes / deletion vectors reference, this attaches
     * {@code validateDataFilesExist(referencedDataFiles).validateDeletedFiles()} so a delete planned
     * from {@code baseSnapshotId} is rejected (retryable {@code ValidationException}) when a concurrent
     * OVERWRITE / compaction removed any referenced data file since the base snapshot — closing the
     * lost-update / dangling-delete window (G-DML-05 / G-CONN-02 / F-CHAIN-WRITE-02). When the manifest
     * is empty (e.g. equality-only deletes, or a producer that does not yet carry referenced files) the
     * extra checks are skipped and behaviour matches the snapshot-only path.
     *
     * @param update              the in-progress RowDelta commit
     * @param baseSnapshotId      snapshot the write was planned from, or a negative sentinel when none
     * @param referencedDataFiles paths of data files the deletes reference (may be empty, not null)
     * @return the same builder, with validations attached when a base snapshot is present
     */
    public static RowDelta validate(RowDelta update, long baseSnapshotId,
                                    Iterable<? extends CharSequence> referencedDataFiles) {
        if (baseSnapshotId < 0) {
            return update;
        }
        update.validateFromSnapshot(baseSnapshotId);
        if (referencedDataFiles != null && referencedDataFiles.iterator().hasNext()) {
            update.validateDataFilesExist(referencedDataFiles)
                    .validateDeletedFiles();
        }
        return update;
    }
}
