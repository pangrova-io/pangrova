package org.pangrova.coordinator.integration.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.paimon.PaimonMetadata;
import org.pangrova.connector.paimon.PaimonPageSourceProvider;
import org.pangrova.connector.paimon.PaimonSplitManager;
import org.pangrova.connector.paimon.PaimonSplitRegistry;
import org.pangrova.coordinator.task.TestConnectorQuery;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SDK component values and snapshot retention, not evidence of a Coordinator production row-reading route. */
class PaimonScopedSplitExecutionTest {
    @TempDir Path warehouse;

    @Test
    void shutdownRetainsAnInFlightActualSdkBindingUntilItsIoExits() throws Exception {
        var options = new Options();
        options.setString("warehouse", warehouse.toString());
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
                var registry = new PaimonSplitRegistry()) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "pending");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .option("bucket", "-1").option("file.format", "parquet").build(), false);
            write(catalog.getTable(id), 17);
            var entered = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            // All SDK behavior remains the actual catalog; only the getTable scheduling is instrumented.
            var observed = new org.apache.paimon.catalog.DelegateCatalog(catalog) {
                @Override
                public org.apache.paimon.catalog.CatalogLoader catalogLoader() {
                    return catalog.catalogLoader();
                }

                @Override
                public Table getTable(Identifier identifier) throws TableNotExistException {
                    if (armed.get()) {
                        entered.countDown();
                        try {
                            if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                                throw new IllegalStateException("SDK lookup release timed out");
                            }
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(failure);
                        }
                    }
                    return super.getTable(identifier);
                }
            };
            var query = new TestConnectorQuery("pending-binding");
            var actor = query.session("bound", "db");
            var metadata = new PaimonMetadata(observed, "bound", registry);
            query.authority().beginMetadata("bound", () -> metadata);
            var handle = metadata.getTableHandle(actor, new SchemaTableName("db", "pending"));
            armed.set(true);
            var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
            try {
                var binding = executor.submit(() -> metadata.bindRead(actor, handle));
                assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(registry::close).hasMessageContaining("active read binding resolution");
                query.seal();
                assertThatThrownBy(() -> query.authority().cleanupMetadata()).hasMessageContaining("not exited");
                release.countDown();
                var actual = binding.get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(actual.snapshotId()).isPresent();
                assertThat(actual.statistics().rowCount()).isEqualTo(1);
                query.authority().cleanupMetadata().requireSuccess();
                registry.close();
                query.retire();
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void perFileRegistrationsReadEachSelectedFileExactlyOnceAfterAnotherCommit() throws Exception {
        var options = new Options();
        options.setString("warehouse", warehouse.toString());
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
                var registry = new PaimonSplitRegistry()) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "rows");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .option("bucket", "-1").option("file.format", "parquet")
                    .option("write-only", "true").option("scan.split.target-size", "128mb").build(), false);
            Table table = catalog.getTable(id);
            write(table, 17);
            write(table, 23);
            var sdkSplits = table.newReadBuilder().newScan().plan().splits();
            assertThat(sdkSplits).anySatisfy(split -> {
                assertThat(split).isInstanceOf(DataSplit.class);
                assertThat(((DataSplit) split).dataFiles()).hasSize(2);
            });
            var query = new TestConnectorQuery("selected-two-files");
            var session = query.session("bound", "db");
            var metadata = new PaimonMetadata(catalog, "bound", registry);
            query.authority().beginMetadata("bound", () -> metadata);
            var binding = metadata.bindRead(session,
                    metadata.getTableHandle(session, new SchemaTableName("db", "rows")));
            assertThat(binding.statistics().rowCount()).isEqualTo(2);
            write(table, 99);
            var manager = new PaimonSplitManager(catalog, registry);
            var source = manager.getSplits(ConnectorTransactionHandle.EMPTY, session, binding.handle(),
                    DynamicFilter.ALL, Constraint.EMPTY);
            var splits = new ArrayList<org.pangrova.spi.connector.metadata.ConnectorSplit>();
            try {
                while (!source.isFinished()) splits.addAll(source.getNextBatch(100));
            } finally {
                source.close();
            }
            assertThat(splits).hasSize(2);
            var provider = new PaimonPageSourceProvider(registry);
            var columns = List.copyOf(metadata.getColumnHandles(session, binding.handle()).values());
            var actualValues = new ArrayList<Integer>();
            for (var split : splits) {
                var reader = provider.createPageSource(ConnectorTransactionHandle.EMPTY, session, split,
                        binding.handle(), columns, DynamicFilter.ALL);
                try {
                    while (!reader.isFinished()) {
                        var page = reader.getNextPage();
                        if (page != null) {
                            page.rows().forEach(row -> actualValues.add(((Number) row.getRaw("id")).intValue()));
                        }
                    }
                } finally {
                    reader.close();
                }
            }
            assertThat(actualValues).containsExactlyInAnyOrder(17, 23);
            assertThat(registry.size()).isZero();
            query.seal();
            query.authority().cleanupMetadata().requireSuccess();
            query.retire();
        }
    }

    private static void write(Table table, int value) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var write = builder.newWrite(); var commit = builder.newCommit()) {
            write.write(GenericRow.of(value));
            commit.commit(write.prepareCommit());
        }
    }
}
