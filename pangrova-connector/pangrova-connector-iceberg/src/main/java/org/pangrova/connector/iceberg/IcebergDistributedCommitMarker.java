package org.pangrova.connector.iceberg;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;

import java.util.HashSet;
import java.util.Set;

/** Persists and verifies the journaled-write marker in Iceberg snapshot summaries. */
final class IcebergDistributedCommitMarker {

    static final String FINGERPRINT = "pangrova.commit.fingerprint";
    static final String EXECUTION_ID = "pangrova.commit.execution-id";
    static final String TENANT_ID = "pangrova.commit.tenant-id";
    static final String BASE_SNAPSHOT_ID = "pangrova.commit.base-snapshot-id";
    static final String COPY_FILE_KEY = "pangrova.copy.file-key";
    static final String COPY_LOAD_GENERATION = "pangrova.copy.load-generation";
    static final String COPY_CLAIM_DIGEST = "pangrova.copy.claim-digest";
    static final String COPY_TABLE_UUID = "pangrova.copy.table-uuid";

    private IcebergDistributedCommitMarker() {
    }

    static boolean isPublished(Table table, DistributedWriteCommitContext context) {
        return publishedSnapshot(table, context) != null;
    }

    static Snapshot publishedSnapshot(Table table, DistributedWriteCommitContext context) {
        if (context == null) {
            return null;
        }
        Snapshot matched = null;
        for (Snapshot snapshot : table.snapshots()) {
            if (context.commitFingerprint().equals(snapshot.summary().get(FINGERPRINT))) {
                requireSummary(snapshot, EXECUTION_ID, context.executionId());
                requireSummary(snapshot, TENANT_ID, context.tenantId());
                requireSummary(snapshot, BASE_SNAPSHOT_ID, String.valueOf(context.baseSnapshotId()));
                if (context.copyFile() != null) {
                    requireSummary(snapshot, COPY_FILE_KEY, context.copyFile().fileKey());
                    requireSummary(snapshot, COPY_LOAD_GENERATION,
                            String.valueOf(context.copyFile().loadGeneration()));
                    requireSummary(snapshot, COPY_CLAIM_DIGEST, context.copyFile().claimDigest());
                    requireSummary(snapshot, COPY_TABLE_UUID, context.copyFile().tableUuid());
                }
                if (matched != null) {
                    throw new IllegalStateException(
                            "Iceberg commit fingerprint appears in multiple snapshots");
                }
                matched = snapshot;
            }
        }
        if (matched != null) {
            Snapshot current = table.currentSnapshot();
            if (current == null || !SnapshotUtil.isAncestorOf(
                    table, current.snapshotId(), matched.snapshotId())) {
                throw new IllegalStateException(
                        "Iceberg commit fingerprint is not in the current snapshot ancestry; "
                                + "commit outcome is ambiguous");
            }
        }
        return matched;
    }

    static void requireCompleteHistorySinceBase(
            Table table, DistributedWriteCommitContext context) {
        if (context == null) {
            return;
        }
        long baseId = context.baseSnapshotId();
        Snapshot snapshot = table.currentSnapshot();
        Set<Long> visited = new HashSet<>();
        while (snapshot != null) {
            if (!visited.add(snapshot.snapshotId())) {
                throw new IllegalStateException(
                        "Iceberg snapshot ancestry contains a cycle; commit outcome is ambiguous");
            }
            if (snapshot.snapshotId() == baseId) {
                return;
            }
            Long parentId = snapshot.parentId();
            if (parentId == null) {
                if (baseId < 0) {
                    return;
                }
                break;
            }
            snapshot = table.snapshot(parentId);
            if (snapshot == null) {
                throw new IllegalStateException(
                        "Iceberg snapshot ancestry was expired before commit reconciliation; "
                                + "commit outcome is ambiguous");
            }
        }
        if (baseId >= 0) {
            throw new IllegalStateException(
                    "Iceberg base snapshot is no longer in the current ancestry; "
                            + "commit outcome is ambiguous");
        }
    }

    static <T> void apply(
            SnapshotUpdate<T> update, DistributedWriteCommitContext context) {
        if (context == null) {
            return;
        }
        update.set(FINGERPRINT, context.commitFingerprint());
        update.set(EXECUTION_ID, context.executionId());
        update.set(TENANT_ID, context.tenantId());
        update.set(BASE_SNAPSHOT_ID, String.valueOf(context.baseSnapshotId()));
        if (context.copyFile() != null) {
            update.set(COPY_FILE_KEY, context.copyFile().fileKey());
            update.set(COPY_LOAD_GENERATION, String.valueOf(context.copyFile().loadGeneration()));
            update.set(COPY_CLAIM_DIGEST, context.copyFile().claimDigest());
            update.set(COPY_TABLE_UUID, context.copyFile().tableUuid());
        }
    }

    private static void requireSummary(Snapshot snapshot, String key, String expected) {
        String actual = snapshot.summary().get(key);
        if (!expected.equals(actual)) {
            throw new IllegalStateException(
                    "Iceberg commit fingerprint metadata mismatch for snapshot "
                            + snapshot.snapshotId() + " key=" + key);
        }
    }
}
