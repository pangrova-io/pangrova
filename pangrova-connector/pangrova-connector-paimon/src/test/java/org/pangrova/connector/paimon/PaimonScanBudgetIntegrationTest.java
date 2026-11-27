package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
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

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

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

            var limited = new PaimonScanBudget(SplitPlanningBudget.standard(), () -> false, 1);
            assertThatThrownBy(() -> limited.plan(table, null))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> {
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED);
                        assertThat(failure).hasMessageContaining("candidate manifest entries");
                    });
            var allowed = new PaimonScanBudget(SplitPlanningBudget.standard(), () -> false, 2);
            assertThat(allowed.plan(table, null).stream()
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

            var limited = new PaimonScanBudget(SplitPlanningBudget.standard(), () -> false, 1);
            assertThatThrownBy(() -> limited.plan(table, null))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> {
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED);
                        assertThat(failure).hasMessageContaining("candidate manifest entries");
                    });
            var allowed = new PaimonScanBudget(SplitPlanningBudget.standard(), () -> false, 2);
            assertThat(allowed.plan(table, null)).isEqualTo(table.newReadBuilder().newScan().plan().splits());
            var filter = new org.apache.paimon.predicate.PredicateBuilder(table.rowType()).equal(0, 1L);
            assertThat(allowed.plan(table, filter))
                    .isEqualTo(table.newReadBuilder().withFilter(filter).newScan().plan().splits());
            assertCancellationDuringManifestRead(table);
        }
    }

    private static void assertCancellationDuringManifestRead(FileStoreTable table) {
        var owner = Thread.currentThread();
        var closed = new AtomicBoolean();
        var budget = new PaimonScanBudget(SplitPlanningBudget.standard(), () -> {
            if (Thread.currentThread() != owner) closed.set(true);
            return closed.get();
        }, 2);
        assertThatThrownBy(() -> budget.plan(table, null))
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
