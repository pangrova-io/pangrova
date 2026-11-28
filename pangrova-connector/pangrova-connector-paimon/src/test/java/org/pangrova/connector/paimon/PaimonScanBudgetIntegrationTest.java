package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.Domain;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.SplitPlanningBudget.EncodedMetadataBudget;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonScanBudgetIntegrationTest {
    @TempDir Path directory;

    @Test
    void actualManifestReadsRejectCandidateOverflowWithoutReturningAShortenedPlan() throws Exception {
        var options = new Options();
        options.setString("warehouse", directory.toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "bounded");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .option("bucket", "-1").option("file.format", "parquet").build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            write(table, 1);
            write(table, 2);

            var limited = budget(1);
            assertThatThrownBy(() -> limited.plan(table, null, null))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> {
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED);
                        assertThat(failure).hasMessageContaining("manifest");
                    });
            var allowed = budget(2);
            assertThat(allowed.plan(table, null, null).stream()
                    .map(DataSplit.class::cast).mapToInt(split -> split.dataFiles().size()).sum()).isEqualTo(2);
            allowed.withPlanningTable(table, null, (view, io) -> {
                assertThat(view.fileIO()).isSameAs(io);
                assertThatThrownBy(() -> {
                    try (var output = new ObjectOutputStream(OutputStream.nullOutputStream())) {
                        output.writeObject(io);
                    }
                }).isInstanceOf(NotSerializableException.class).hasMessageContaining("active query");
                return List.of();
            });
            assertCancellationDuringManifestRead(table);
        }
    }

    @Test
    void dataEvolutionScanRejectsCandidateOverflowWithoutDroppingItsWrapper() throws Exception {
        var options = new Options();
        options.setString("warehouse", directory.toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "evolution");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .option("bucket", "-1").option("file.format", "parquet")
                    .option("data-evolution.enabled", "true").option("row-tracking.enabled", "true")
                    .build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            write(table, 1);
            write(table, 2);

            var limited = budget(1);
            assertThatThrownBy(() -> limited.plan(table, null, null))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> {
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED);
                        assertThat(failure).hasMessageContaining("manifest");
                    });
            var allowed = budget(2);
            assertThat(allowed.plan(table, null, null)).isEqualTo(table.newReadBuilder().newScan().plan().splits());
            var filter = new org.apache.paimon.predicate.PredicateBuilder(table.rowType()).equal(0, 1L);
            assertThat(allowed.plan(table, filter, null))
                    .isEqualTo(table.newReadBuilder().withFilter(filter).newScan().plan().splits());
            assertCancellationDuringManifestRead(table);
        }
    }

    @Test
    void openLimitPreventsSdkFromOpeningTheRestOfTheManifestBatch() throws Exception {
        var io = new ManifestIo();
        try (var catalog = new FileSystemCatalog(io, new org.apache.paimon.fs.Path(directory.toString()))) {
            var table = manifestTable(catalog, 12);
            io.observe = true;
            assertThatThrownBy(() -> budget(1).plan(table, null, null))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> {
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED);
                        assertThat(failure).hasMessageContaining("manifest opens");
                    });
            assertThat(io.opens.get()).isOne();
            var allowed = budget(12).plan(table, null, null);
            assertThat(allowed).isEqualTo(table.newReadBuilder().newScan().plan().splits());
        }
    }

    @Test
    void encodedByteLimitRejectsMetadataBeforeDecodingThePlan() throws Exception {
        var io = new ManifestIo();
        try (var catalog = new FileSystemCatalog(io, new org.apache.paimon.fs.Path(directory.toString()))) {
            var table = manifestTable(catalog, 3);
            io.observe = true;
            var budget = budget(stageBudget(1), 100);
            assertThatThrownBy(() -> budget.plan(table, null, null))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> {
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED);
                        assertThat(failure).hasMessageContaining("encoded metadata read budget");
                    });
            assertThat(io.opens.get()).isZero();
            var incremental = table.copyWithoutTimeTravel(Map.of("scan.mode", "incremental",
                    "incremental-between", "1,3"));
            assertThat(budget(100).plan(incremental, null, null))
                    .isEqualTo(incremental.newReadBuilder().newScan().plan().splits());
        }
    }

    @Test
    void twoPhysicalSourcesChargeTheSameStageBudget() throws Exception {
        var warehouse = new org.apache.paimon.fs.Path(directory.toString());
        var firstId = Identifier.create("db", "first_stage_source");
        var secondId = Identifier.create("db", "second_stage_source");
        try (var catalog = new FileSystemCatalog(new LocalFileIO(), warehouse)) {
            manifestTable(catalog, firstId, 12);
            manifestTable(catalog, secondId, 16);
        }

        var baseline = SplitPlanningBudget.standard();
        long initialBytes = baseline.encodedMetadata().remainingBytes();
        long firstSourceBytes;
        long secondSourceBytes;
        try (var catalog = new FileSystemCatalog(new LocalFileIO(), warehouse)) {
            assertThat(budget(baseline, 100).plan((FileStoreTable) catalog.getTable(firstId), null, null))
                    .isNotEmpty();
            long remainingAfterFirst = baseline.encodedMetadata().remainingBytes();
            firstSourceBytes = initialBytes - remainingAfterFirst;
            assertThat(budget(baseline, 100).plan((FileStoreTable) catalog.getTable(secondId), null, null))
                    .isNotEmpty();
            secondSourceBytes = remainingAfterFirst - baseline.encodedMetadata().remainingBytes();
        }
        assertThat(firstSourceBytes).isPositive();
        assertThat(secondSourceBytes).isGreaterThan(firstSourceBytes);

        // Each scan fits independently. The first scan consumes more than the spare credit for short reads.
        long stageBytes = secondSourceBytes + firstSourceBytes / 2;
        long bytesLeftForSecond = stageBytes - firstSourceBytes;
        var stage = stageBudget(stageBytes);
        try (var catalog = new FileSystemCatalog(new LocalFileIO(), warehouse)) {
            var first = budget(stage.forSource(1000, stage.fileGranularity()), 100);
            assertThat(first.plan((FileStoreTable) catalog.getTable(firstId), null, null)).isNotEmpty();
            assertThat(stage.encodedMetadata().remainingBytes()).isEqualTo(bytesLeftForSecond);

            var second = budget(stage.forSource(500, stage.fileGranularity()), 100);
            var secondTable = (FileStoreTable) catalog.getTable(secondId);
            assertThatThrownBy(() -> second.plan(secondTable, null, null))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> {
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED);
                        assertThat(failure).hasMessageContaining("Stage encoded metadata read budget exhausted");
                    });
            assertThat(stage.encodedMetadata().remainingBytes()).isLessThan(bytesLeftForSecond);
        }
        try (var catalog = new FileSystemCatalog(new LocalFileIO(), warehouse)) {
            assertThat(budget(stageBudget(stageBytes), 100)
                    .plan((FileStoreTable) catalog.getTable(secondId), null, null)).isNotEmpty();
        }
    }

    @Test
    void interruptedPlannerKeepsQueryPermitUntilBackendOpensActuallyExit() throws Exception {
        var io = new ManifestIo();
        try (var catalog = new FileSystemCatalog(io, new org.apache.paimon.fs.Path(directory.toString()))) {
            var table = manifestTable(catalog, 4);
            io.observe = true;
            io.hold = true;
            var releases = new AtomicInteger();
            var drained = new CountDownLatch(1);
            var failure = new AtomicReference<Throwable>();
            Thread owner = new Thread(() -> {
                var budget = new PaimonScanBudget(SplitPlanningBudget.standard(), () -> false, () -> () -> {
                    releases.incrementAndGet();
                    drained.countDown();
                }, 100);
                try {
                    budget.plan(table, null, null);
                } catch (RuntimeException error) {
                    failure.set(error);
                }
            }, "paimon-planning-owner-test");
            owner.setDaemon(true);
            owner.start();
            try {
                assertThat(io.entered.await(10, TimeUnit.SECONDS)).isTrue();
                owner.interrupt();
                owner.join(5_000);
                assertThat(owner.isAlive()).isFalse();
                assertThat(failure.get()).isInstanceOfSatisfying(ConnectorException.class, error ->
                        assertThat(error.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED));
                assertThat(releases.get()).isZero();
            } finally {
                io.resume.countDown();
                owner.join(5_000);
            }
            assertThat(drained.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(releases.get()).isOne();
            assertThat(io.opens.get()).isEqualTo(2);
        }
    }

    @Test
    void blockedManifestPreventsSubmissionBeyondTheCurrentWindow() throws Exception {
        var io = new ManifestIo();
        try (var catalog = new FileSystemCatalog(io, new org.apache.paimon.fs.Path(directory.toString()))) {
            var table = manifestTable(catalog, 5);
            io.observe = true;
            io.holdOnlyFirst = true;
            var planner = ManagedExecutorFactory.newSingleThreadExecutor("bounded-paimon-manifest-test");
            try {
                var planned = planner.submit(() -> budget(100).plan(table, null, null));
                assertThat(io.entered.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(io.beyondWindow.await(500, TimeUnit.MILLISECONDS)).isFalse();
                io.resume.countDown();
                assertThat(planned.get(15, TimeUnit.SECONDS).stream()
                        .map(DataSplit.class::cast).mapToInt(split -> split.dataFiles().size()).sum()).isEqualTo(5);
                assertThat(io.opens.get()).isEqualTo(5);
            } finally {
                io.resume.countDown();
                planner.shutdownNow();
                assertThat(planner.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void deletesInLaterManifestWindowsRemoveFilesFromEarlierWindows() throws Exception {
        var io = new ManifestIo();
        try (var catalog = new FileSystemCatalog(io, new org.apache.paimon.fs.Path(directory.toString()))) {
            var table = manifestTable(catalog, 5);
            write(table.newBatchWriteBuilder().withOverwrite(), 987);
            var expected = table.newScan().plan().splits().stream().map(DataSplit.class::cast)
                    .flatMap(split -> split.dataFiles().stream()).toList();
            io.observe = true;
            var actual = budget(100).plan(table, null, null).stream().map(DataSplit.class::cast)
                    .flatMap(split -> split.dataFiles().stream()).toList();
            assertThat(expected).hasSize(1);
            assertThat(actual).containsExactlyElementsOf(expected);
            assertThat(io.opens.get()).isGreaterThan(2);
        }
    }

    @ParameterizedTest
    @EnumSource(value = DataTypeRoot.class, names = {"TINYINT", "SMALLINT", "INTEGER"})
    void pinnedPaimonPlanningPrunesDisjointSignedBuildRanges(DataTypeRoot width) throws Exception {
        var type = switch (width) {
            case TINYINT -> DataTypes.TINYINT();
            case SMALLINT -> DataTypes.SMALLINT();
            case INTEGER -> DataTypes.INT();
            default -> throw new AssertionError(width);
        };
        try (var catalog = new FileSystemCatalog(new LocalFileIO(), new org.apache.paimon.fs.Path(directory.toString()))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "signed_ranges");
            catalog.createTable(id, Schema.newBuilder().column("id", type)
                    .column("payload", DataTypes.BIGINT()).partitionKeys("id").option("bucket", "-1")
                    .build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            var builder = table.newBatchWriteBuilder();
            try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
                for (int key = -4; key < 5; key++) {
                    Object value = switch (width) {
                        case TINYINT -> Byte.valueOf((byte) key);
                        case SMALLINT -> Short.valueOf((short) key);
                        case INTEGER -> Integer.valueOf(key);
                        default -> throw new AssertionError(width);
                    };
                    writer.write(GenericRow.of(value, (long) key * 10));
                }
                commit.commit(writer.prepareCommit());
            }
            var snapshot = table.snapshotManager().latestSnapshot();
            var domain = new Domain.RangeUnion(List.of(
                    new Domain.Range(-3L, -2L, true, true), new Domain.Range(3L, 3L, true, true)));
            ColumnHandle key = () -> "id";
            var predicate = PaimonFilterBuilder.build(table.rowType(), () -> Map.of(key, domain));
            var filtered = budget(100).plan(table, predicate, snapshot).stream()
                    .map(DataSplit.class::cast).map(DataSplit::partition).map(partition -> switch (width) {
                        case TINYINT -> (int) partition.getByte(0);
                        case SMALLINT -> (int) partition.getShort(0);
                        case INTEGER -> partition.getInt(0);
                        default -> throw new AssertionError(width);
                    }).toList();

            assertThat(budget(100).plan(table, null, snapshot)).hasSize(9);
            assertThat(filtered).containsExactlyInAnyOrder(-3, -2, 3);
        }
    }

    private static FileStoreTable manifestTable(FileSystemCatalog catalog, int count) throws Exception {
        return manifestTable(catalog, Identifier.create("db", "bounded_manifest_io"), count);
    }

    private static FileStoreTable manifestTable(FileSystemCatalog catalog, Identifier id, int count) throws Exception {
        catalog.createDatabase("db", true);
        catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .option("bucket", "-1").option("file.format", "parquet")
                .option("manifest.merge-min-count", "1000")
                .option("scan.manifest.parallelism", "2").build(), false);
        var table = (FileStoreTable) catalog.getTable(id);
        for (int idValue = 0; idValue < count; idValue++) write(table, idValue);
        return table;
    }

    private static PaimonScanBudget budget(int maxCandidates) {
        return budget(SplitPlanningBudget.standard(), maxCandidates);
    }

    private static PaimonScanBudget budget(SplitPlanningBudget stage, int maxCandidates) {
        return new PaimonScanBudget(stage, () -> false, () -> () -> { }, maxCandidates);
    }

    private static SplitPlanningBudget stageBudget(long bytes) {
        var standard = SplitPlanningBudget.standard();
        return new SplitPlanningBudget(standard.deadlineNanos(), standard.remainingSplits(),
                standard.fileGranularity(), new EncodedMetadataBudget(bytes));
    }

    private static final class ManifestIo extends LocalFileIO {
        private final AtomicInteger opens = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(2);
        private final CountDownLatch resume = new CountDownLatch(1);
        private final CountDownLatch beyondWindow = new CountDownLatch(1);
        private volatile boolean observe;
        private volatile boolean hold;
        private volatile boolean holdOnlyFirst;

        @Override
        public SeekableInputStream newInputStream(org.apache.paimon.fs.Path path) throws IOException {
            if (observe && path.getName().startsWith("manifest-") && !path.getName().startsWith("manifest-list-")) {
                int opened = opens.incrementAndGet();
                if (opened > 2) beyondWindow.countDown();
                if (hold || holdOnlyFirst) entered.countDown();
                if (hold || holdOnlyFirst && opened == 1) {
                    try {
                        if (!resume.await(15, TimeUnit.SECONDS)) throw new IOException("Blocked manifest test timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IOException(interrupted);
                    }
                }
            }
            return super.newInputStream(path);
        }
    }

    private static void assertCancellationDuringManifestRead(FileStoreTable table) {
        var owner = Thread.currentThread();
        var closed = new AtomicBoolean();
        var budget = new PaimonScanBudget(SplitPlanningBudget.standard(), () -> {
            if (Thread.currentThread() != owner) closed.set(true);
            return closed.get();
        }, () -> () -> { }, 2);
        assertThatThrownBy(() -> budget.plan(table, null, null))
                .isInstanceOfSatisfying(ConnectorException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED));
        assertThat(closed).isTrue();
    }

    private static void write(FileStoreTable table, long value) throws Exception {
        write(table.newBatchWriteBuilder(), value);
    }

    private static void write(BatchWriteBuilder builder, long value) throws Exception {
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            writer.write(GenericRow.of(value));
            commit.commit(writer.prepareCommit());
        }
    }
}
