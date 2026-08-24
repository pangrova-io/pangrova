package org.pangrova.coordinator.query.task;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A-COMMIT-01: verifies the recovery reconciler AUTO-FINISHES recoverable distributed-write intents
 * (rehydrate handle + re-drive the idempotent catalog commit) and only quarantines/aborts the
 * genuinely unrecoverable ones — replacing the previous always-QUARANTINE behavior.
 */
class DefaultWriteCommitReconcilerTest {

    private static final String CATALOG = "iceberg";
    private static final String TABLE = "sales.orders";

    private static WriteCommitEntry pending(String tableName, List<byte[]> fragments, String writeMode) {
        return new WriteCommitEntry("q1", CATALOG, tableName,
                WriteCommitEntry.State.PENDING, System.currentTimeMillis(),
                fragments, writeMode, "q1", null, 0L);
    }

    @Test
    void rehydratesAndReDrivesCommit() {
        RecordingMetadata md = new RecordingMetadata();
        var reconciler = new DefaultWriteCommitReconciler((c, s) -> md);

        var outcome = reconciler.reconcile(pending(TABLE, List.of("frag".getBytes()), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.COMMITTED);
        assertThat(md.reconstructCount.get()).as("handle rehydrated").isEqualTo(1);
        assertThat(md.finishCount.get()).as("commit re-driven (idempotent)").isEqualTo(1);
        assertThat(md.lastFragments).as("original worker fragments replayed").hasSize(1);
        assertThat(md.lastTable).isEqualTo(new SchemaTableName("sales", "orders"));
        assertThat(md.lastWriteMode).isEqualTo("INSERT");
    }

    @Test
    void connectorCannotRehydrate_quarantined() {
        RecordingMetadata md = new RecordingMetadata();
        md.unsupportedReconstruct = true;
        var reconciler = new DefaultWriteCommitReconciler((c, s) -> md);

        var outcome = reconciler.reconcile(pending(TABLE, List.of("f".getBytes()), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
        assertThat(md.finishCount.get()).as("must not commit when rehydrate unsupported").isZero();
    }

    @Test
    void tableGone_aborted() {
        RecordingMetadata md = new RecordingMetadata();
        md.tableGone = true;
        var reconciler = new DefaultWriteCommitReconciler((c, s) -> md);

        var outcome = reconciler.reconcile(pending(TABLE, List.of("f".getBytes()), "DELETE"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.ABORTED);
        assertThat(md.finishCount.get()).isZero();
    }

    @Test
    void noFragments_quarantinedWithoutTouchingCatalog() {
        RecordingMetadata md = new RecordingMetadata();
        var reconciler = new DefaultWriteCommitReconciler((c, s) -> md);

        var outcome = reconciler.reconcile(pending(TABLE, List.of(), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
        assertThat(md.reconstructCount.get()).as("no replay payload -> do not touch catalog").isZero();
    }

    @Test
    void legacyUnparseableIdentity_quarantined() {
        RecordingMetadata md = new RecordingMetadata();
        var reconciler = new DefaultWriteCommitReconciler((c, s) -> md);

        // A legacy lossy handle.toString() cannot be reconstructed -> safe quarantine.
        var outcome = reconciler.reconcile(
                pending("IcebergOutputTableHandle[schemaName=x]", List.of("f".getBytes()), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
        assertThat(md.reconstructCount.get()).isZero();
    }

    @Test
    void unknownCatalog_quarantined() {
        var reconciler = new DefaultWriteCommitReconciler((c, s) -> null);

        var outcome = reconciler.reconcile(pending(TABLE, List.of("f".getBytes()), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
    }

    // --- test doubles ---

    /** Simple-name contains "NoSuchTable" so the reconciler's table-gone heuristic matches. */
    private static final class NoSuchTableException extends RuntimeException {
        NoSuchTableException() {
            super("table does not exist");
        }
    }

    private static final class RecordingMetadata implements ConnectorMetadata {
        final AtomicInteger reconstructCount = new AtomicInteger();
        final AtomicInteger finishCount = new AtomicInteger();
        volatile boolean unsupportedReconstruct = false;
        volatile boolean tableGone = false;
        volatile List<byte[]> lastFragments;
        volatile SchemaTableName lastTable;
        volatile String lastWriteMode;

        @Override
        public ConnectorOutputTableHandle reconstructOutputHandle(ConnectorSession session,
                SchemaTableName tableName, String writeMode) {
            reconstructCount.incrementAndGet();
            if (unsupportedReconstruct) {
                throw new UnsupportedOperationException("connector cannot rehydrate");
            }
            if (tableGone) {
                throw new NoSuchTableException();
            }
            lastTable = tableName;
            lastWriteMode = writeMode;
            return new TestOutputHandle(tableName, writeMode);
        }

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                ConnectorOutputTableHandle outputHandle, List<byte[]> fragments) {
            finishCount.incrementAndGet();
            lastFragments = fragments;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }
    }

    private record TestOutputHandle(SchemaTableName table, String mode) implements ConnectorOutputTableHandle {
        @Override
        public Optional<SchemaTableName> targetTable() {
            return Optional.of(table);
        }

        @Override
        public String writeMode() {
            return mode;
        }
    }
}
