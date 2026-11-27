package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonReadBindingIoIntegrationTest {
    @TempDir java.nio.file.Path warehouse;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void historicalBindingUsesOneSnapshotForSchemaAndStatisticsThenRevalidates(boolean withStatistics)
            throws Exception {
        var io = new CountingSnapshotIo();
        try (var catalog = new FileSystemCatalog(io, new Path(warehouse.toString()))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "history");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .option("bucket", "-1").option("file.format", "parquet").build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            var builder = table.newBatchWriteBuilder();
            try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
                writer.write(GenericRow.of(10L));
                commit.commit(writer.prepareCommit());
            }
            if (withStatistics) {
                try (var commit = table.newCommit("analyze-history")) {
                    commit.updateStatistics(new Statistics(1, table.schema().id(), 1L, 8L,
                            Map.of("id", ColStats.<Long>newColStats(0, 1L, 10L, 10L, 0L, 8L, 8L))));
                }
            }
            long snapshotId = table.snapshotManager().latestSnapshotId();
            catalog.alterTable(id, SchemaChange.addColumn("later", DataTypes.STRING()), false);
            var selector = PaimonTableHandleFactory.fromTable("db", "history", catalog.getTable(id))
                    .withTimeTravelSnapshot(Long.toString(snapshotId));
            io.snapshotPath = table.snapshotManager().snapshotPath(snapshotId);

            var bound = PaimonReadBindings.bind(catalog, selector);
            assertThat(bound.declaration().metadata().columns()).containsExactly("id");
            assertThat(bound.statistics().rowCount()).isEqualTo(1);
            assertThat(bound.statistics().columnStats().get("id").ndv()).isEqualTo(withStatistics ? 1 : -1);
            assertThat(new int[] {io.snapshotExists, io.snapshotOpens})
                    .as("snapshot existence probes, snapshot opens").containsExactly(0, 2);

            io.deleteAfterRead = true;
            assertThatThrownBy(() -> PaimonReadBindings.bind(catalog, selector))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> assertThat(failure.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND));
        }
    }

    private static final class CountingSnapshotIo extends LocalFileIO {
        private Path snapshotPath;
        private int snapshotOpens;
        private int snapshotExists;
        private boolean deleteAfterRead;

        @Override
        public boolean exists(Path path) throws IOException {
            if (path.equals(snapshotPath)) snapshotExists++;
            return super.exists(path);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (path.equals(snapshotPath)) snapshotOpens++;
            return super.newInputStream(path);
        }

        @Override
        public String readFileUtf8(Path path) throws IOException {
            String text = super.readFileUtf8(path);
            if (deleteAfterRead && path.equals(snapshotPath)) {
                deleteAfterRead = false;
                delete(path, false);
            }
            return text;
        }
    }
}
