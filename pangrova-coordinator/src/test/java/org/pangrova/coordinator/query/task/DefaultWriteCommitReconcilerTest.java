package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.task.write.WriteCommitReconciler;

import org.pangrova.coordinator.query.execution.ConnectorWriteBindingFactory;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitFingerprint;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies recovery uses the exact Worker binding, never live-table reconstruction.
 */
class DefaultWriteCommitReconcilerTest {

    private static final String CATALOG = "iceberg";
    private static final String TABLE = "sales.orders";

    private static ConnectorCatalogHandle catalog(RecordingMetadata metadata) {
        return catalog(metadata, "catalog-incarnation-1");
    }

    private static ConnectorCatalogHandle catalog(
            RecordingMetadata metadata, String incarnation) {
        CatalogDescriptor descriptor = new CatalogDescriptor(
                CATALOG, CatalogType.CONNECTOR, "test-connector", CatalogState.ACTIVE,
                Map.of(), "", Set.of(), incarnation, 1L);
        return new ConnectorCatalogHandle(descriptor, new Connector() {
            @Override public ConnectorMetadata getMetadata(ConnectorSession session) {
                return metadata;
            }
            @Override public ConnectorSplitManager getSplitManager() {
                throw new UnsupportedOperationException("not used by recovery");
            }
            @Override public ConnectorPageSourceProvider getPageSourceProvider() {
                throw new UnsupportedOperationException("not used by recovery");
            }
            @Override public ConnectorOutputHandleCodec getOutputHandleCodec() {
                if (metadata.codecUnavailable) {
                    throw new UnsupportedOperationException("codec unavailable");
                }
                return new TestOutputCodec(metadata.decodeCount);
            }
        });
    }

    private static WriteCommitEntry pending(String tableName, List<byte[]> fragments, String writeMode) {
        byte[] binding = ConnectorWriteBindingFactory.create(
                catalog(new RecordingMetadata()),
                new TestOutputHandle(new SchemaTableName("sales", "orders"), writeMode, false))
                .toByteArray();
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        CATALOG,
                        tableName,
                        writeMode,
                        37L,
                        binding,
                        "alice",
                        org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt()),
                fragments);
        return new WriteCommitEntry(
                "q1",
                CATALOG,
                tableName,
                WriteCommitEntry.State.PENDING,
                System.currentTimeMillis(),
                fragments,
                writeMode,
                fingerprint,
                "tenant-a",
                "alice",
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                37L,
                binding,
                fingerprint,
                null,
                null,
                0L,
                null);
    }

    @Test
    void rehydratesAndReDrivesCommit() {
        RecordingMetadata md = new RecordingMetadata();
        var reconciler = new DefaultWriteCommitReconciler(c -> catalog(md));

        var outcome = reconciler.reconcile(pending(TABLE, List.of("frag".getBytes()), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.COMMITTED);
        assertThat(md.decodeCount.get()).as("exact handle decoded").isEqualTo(1);
        assertThat(md.finishCount.get()).as("commit re-driven (idempotent)").isEqualTo(1);
        assertThat(md.lastFragments).as("original worker fragments replayed").hasSize(1);
        assertThat(md.lastTable).isEqualTo(new SchemaTableName("sales", "orders"));
        assertThat(md.lastWriteMode).isEqualTo("INSERT");
    }

    @Test
    void connectorCannotRehydrate_quarantined() {
        RecordingMetadata md = new RecordingMetadata();
        md.codecUnavailable = true;
        var reconciler = new DefaultWriteCommitReconciler(c -> catalog(md));

        var outcome = reconciler.reconcile(pending(TABLE, List.of("f".getBytes()), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
        assertThat(md.finishCount.get()).as("must not commit when codec unavailable").isZero();
    }

    @Test
    void tableGone_aborted() {
        RecordingMetadata md = new RecordingMetadata();
        md.tableGone = true;
        var reconciler = new DefaultWriteCommitReconciler(c -> catalog(md));

        var outcome = reconciler.reconcile(pending(TABLE, List.of("f".getBytes()), "DELETE"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.ABORTED);
        assertThat(md.finishCount.get()).isZero();
    }

    @Test
    void emptyOverwriteIsReplayedBecauseTruncateHasNoFragments() {
        RecordingMetadata md = new RecordingMetadata();
        var reconciler = new DefaultWriteCommitReconciler(c -> catalog(md));

        var outcome = reconciler.reconcile(pending(TABLE, List.of(), "OVERWRITE"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.COMMITTED);
        assertThat(md.decodeCount.get()).isEqualTo(1);
        assertThat(md.finishCount.get()).isEqualTo(1);
    }

    @Test
    void legacyUnparseableIdentity_quarantined() {
        RecordingMetadata md = new RecordingMetadata();
        var reconciler = new DefaultWriteCommitReconciler(c -> catalog(md));

        // A legacy lossy handle.toString() cannot be reconstructed -> safe quarantine.
        var outcome = reconciler.reconcile(
                pending("IcebergOutputTableHandle[schemaName=x]", List.of("f".getBytes()), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
        assertThat(md.decodeCount.get()).isZero();
    }

    @Test
    void unknownCatalog_quarantined() {
        var reconciler = new DefaultWriteCommitReconciler(c -> null);

        var outcome = reconciler.reconcile(pending(TABLE, List.of("f".getBytes()), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
    }

    @Test
    void sameNameNewCatalogIncarnationIsQuarantinedBeforeCommit() {
        RecordingMetadata md = new RecordingMetadata();
        var reconciler = new DefaultWriteCommitReconciler(
                c -> catalog(md, "catalog-incarnation-2"));

        var outcome = reconciler.reconcile(
                pending(TABLE, List.of("old-fragment".getBytes()), "INSERT"));

        assertThat(outcome).isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
        assertThat(md.decodeCount.get()).isZero();
        assertThat(md.finishCount.get()).isZero();
    }

    @Test
    void nativeAllocationUsesTheSameExactDecoderForNormalCommitAndRecovery() {
        var metadata = new RecordingMetadata();
        var catalog = catalog(metadata);
        var output = new TestOutputHandle(new SchemaTableName("sales", "orders"), "INSERT", true);
        var binding = ConnectorWriteBindingFactory.create(catalog, output);
        assertThat(binding.hasLance()).isTrue();
        var intent = nativeIntent(binding.toByteArray());
        assertThat(DefaultWriteCommitReconciler.decodeExactHandle(intent, output.table(), catalog)).isEqualTo(output);
        assertThat(new DefaultWriteCommitReconciler(ignored -> catalog).reconcile(intent))
                .isEqualTo(WriteCommitReconciler.RecoveryOutcome.COMMITTED);
        assertThat(metadata.finishCount).hasValue(1);
    }

    @Test
    void nativeBindingCannotSubstituteAllocationEvenWhenJournalFingerprintMatches() {
        var metadata = new RecordingMetadata();
        var catalog = catalog(metadata);
        var output = new TestOutputHandle(new SchemaTableName("sales", "orders"), "INSERT", true);
        var canonical = ConnectorWriteBindingFactory.create(catalog, output);
        var changed = canonical.toBuilder().setLance(canonical.getLance().toBuilder()
                .setDatasetUri("file:///warehouse/another-table")).build();
        assertThat(new DefaultWriteCommitReconciler(ignored -> catalog)
                .reconcile(nativeIntent(changed.toByteArray())))
                .isEqualTo(WriteCommitReconciler.RecoveryOutcome.QUARANTINED);
        assertThat(metadata.finishCount).hasValue(0);
    }

    private static WriteCommitEntry nativeIntent(byte[] binding) {
        var fragments = List.of(new byte[]{1});
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        CATALOG,
                        TABLE,
                        "INSERT",
                        37L,
                        binding,
                        "alice",
                        org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt()),
                fragments);
        return new WriteCommitEntry(
                "q1",
                CATALOG,
                TABLE,
                WriteCommitEntry.State.PENDING,
                1L,
                fragments,
                "INSERT",
                fingerprint,
                "tenant-a",
                "alice",
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                37L,
                binding,
                fingerprint,
                null,
                null,
                0L,
                null);
    }

    // --- test doubles ---

    /** Simple-name contains "NoSuchTable" so the reconciler's table-gone heuristic matches. */
    private static final class NoSuchTableException extends RuntimeException {
        NoSuchTableException() {
            super("table does not exist");
        }
    }

    private static final class RecordingMetadata implements ConnectorMetadata {
        final AtomicInteger decodeCount = new AtomicInteger();
        final AtomicInteger finishCount = new AtomicInteger();
        volatile boolean codecUnavailable = false;
        volatile boolean tableGone = false;
        volatile List<byte[]> lastFragments;
        volatile SchemaTableName lastTable;
        volatile String lastWriteMode;

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                ConnectorOutputTableHandle outputHandle, List<byte[]> fragments,
                org.pangrova.spi.connector.metadata.DistributedWriteCommitContext context) {
            if (tableGone) {
                throw new NoSuchTableException();
            }
            finishCount.incrementAndGet();
            lastFragments = fragments;
            lastTable = outputHandle.targetTable().orElseThrow();
            lastWriteMode = outputHandle.writeMode();
            assertThat(context.baseSnapshotId()).isEqualTo(37L);
            assertThat(session.getProperties()).containsEntry("tenant_id", "tenant-a");
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

    private record TestOutputHandle(SchemaTableName table, String mode, boolean nativeOutput)
            implements ConnectorOutputTableHandle {
        @Override
        public Optional<SchemaTableName> targetTable() {
            return Optional.of(table);
        }

        @Override
        public String writeMode() {
            return mode;
        }

        @Override
        public OptionalLong writeBaseSnapshotId() {
            return OptionalLong.of(37L);
        }

        @Override
        public Optional<org.pangrova.spi.connector.metadata.NativeWriteAllocation> nativeWriteAllocation() {
            return nativeOutput ? Optional.of(new org.pangrova.spi.connector.metadata.NativeWriteAllocation(
                    "file:///warehouse/orders", org.pangrova.spi.connector.metadata.NativeWriteAllocation.Operation.APPEND,
                    37L, new byte[32], "write-1", "transactions/write-1", "generation-1", List.of("id")))
                    : Optional.empty();
        }
    }

    private record TestOutputCodec(AtomicInteger decoded) implements ConnectorOutputHandleCodec {
        @Override public String typeUrl() { return "test/output"; }
        @Override public int codecVersion() { return 1; }
        @Override public byte[] serialize(ConnectorOutputTableHandle handle) {
            TestOutputHandle value = (TestOutputHandle) handle;
            return (value.table() + "\n" + value.mode() + "\n" + value.nativeOutput())
                    .getBytes(StandardCharsets.UTF_8);
        }
        @Override public ConnectorOutputTableHandle deserialize(int version, byte[] payload) {
            if (version != codecVersion()) {
                throw new IllegalArgumentException("unsupported codec version");
            }
            String[] parts = new String(payload, StandardCharsets.UTF_8).split("\n", -1);
            if (parts.length != 3 || !TABLE.equals(parts[0])
                    || (!parts[2].equals("true") && !parts[2].equals("false"))) {
                throw new IllegalArgumentException("invalid test output handle");
            }
            decoded.incrementAndGet();
            return new TestOutputHandle(new SchemaTableName("sales", "orders"), parts[1], Boolean.parseBoolean(parts[2]));
        }
        @Override public List<String> writeColumnNames(ConnectorOutputTableHandle handle) {
            return List.of("id");
        }
    }
}
