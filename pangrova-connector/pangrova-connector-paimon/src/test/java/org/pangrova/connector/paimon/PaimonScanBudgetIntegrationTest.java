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
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;

import java.io.IOException;
import java.nio.file.Path;
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
            var budget = new PaimonScanBudget(SplitPlanningBudget.standard(), () -> false, () -> () -> { }, 100, 1L);
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
                }, 100, PaimonScanBudget.MAX_ENCODED_METADATA_BYTES);
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

    private static FileStoreTable manifestTable(FileSystemCatalog catalog, int count) throws Exception {
        var id = Identifier.create("db", "bounded_manifest_io");
        catalog.createDatabase("db", false);
        catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .option("bucket", "-1").option("file.format", "parquet")
                .option("manifest.merge-min-count", "1000")
                .option("scan.manifest.parallelism", "2").build(), false);
        var table = (FileStoreTable) catalog.getTable(id);
        for (int idValue = 0; idValue < count; idValue++) write(table, idValue);
        return table;
    }

    private static PaimonScanBudget budget(int maxCandidates) {
        return new PaimonScanBudget(SplitPlanningBudget.standard(), () -> false, () -> () -> { },
                maxCandidates, PaimonScanBudget.MAX_ENCODED_METADATA_BYTES);
    }

    private static final class ManifestIo extends LocalFileIO {
        private final AtomicInteger opens = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(2);
        private final CountDownLatch resume = new CountDownLatch(1);
        private volatile boolean observe;
        private volatile boolean hold;

        @Override
        public SeekableInputStream newInputStream(org.apache.paimon.fs.Path path) throws IOException {
            if (observe && path.getName().startsWith("manifest-") && !path.getName().startsWith("manifest-list-")) {
                opens.incrementAndGet();
                if (hold) {
                    entered.countDown();
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
        }, () -> () -> { }, 2, PaimonScanBudget.MAX_ENCODED_METADATA_BYTES);
        assertThatThrownBy(() -> budget.plan(table, null, null))
                .isInstanceOfSatisfying(ConnectorException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED));
        assertThat(closed).isTrue();
    }

    private static void write(FileStoreTable table, long value) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            writer.write(GenericRow.of(value));
            commit.commit(writer.prepareCommit());
        }
    }
}
