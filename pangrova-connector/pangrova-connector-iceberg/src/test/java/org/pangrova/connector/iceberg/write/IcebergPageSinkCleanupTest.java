package org.pangrova.connector.iceberg.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.data.page.Page;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Hadoop/Parquet ownership and filesystem errors; no substitute FileIO. */
class IcebergPageSinkCleanupTest {

    @TempDir Path warehouse;

    @Test
    void failedDeletionRemainsObservableAndCanBeRetriedAfterRealFilesystemRecovery() throws Exception {
        Table table = table(false);
        try (var sink = sink(table)) {
            sink.appendPage(rows(1));
            sink.close();
            Path file = files().get(0);
            Path retained = file.resolveSibling(file.getFileName() + ".retained");
            Files.move(file, retained);
            Files.createDirectory(file);
            Path blocker = Files.writeString(file.resolve("actual-delete-barrier"), "occupied");
            try {
                assertThatThrownBy(sink::abort).isInstanceOf(RuntimeException.class);
                assertThat(file).isDirectory();
                assertThatThrownBy(sink::finishWithMetadata)
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("failure");
            } finally {
                Files.delete(blocker);
                Files.delete(file);
                Files.move(retained, file);
            }
            sink.abort();
            sink.abort();
            assertThat(file).doesNotExist();
            assertThat(table.currentSnapshot()).isNull();
        }
    }

    @Test
    void abortDeletesOwnedFilesIncludingPartitionsEvictedFromTheLiveLru() throws Exception {
        Table table = table(true);
        try (var sink = sink(table)) {
            sink.appendPage(rows(AbstractIcebergPageSink.MAX_OPEN_WRITERS + 2));
            sink.close();
            assertThat(files()).hasSize(AbstractIcebergPageSink.MAX_OPEN_WRITERS + 2);
            sink.abort();
            assertThat(files()).isEmpty();
            assertThat(table.currentSnapshot()).isNull();
        }
    }

    @Test
    void preparedFilesAndTheExactFragmentSurviveRepeatedFinishAndAbort() throws Exception {
        Table table = table(false);
        try (var sink = sink(table)) {
            sink.appendPage(rows(2));
            byte[] fragment = sink.finishWithMetadata();
            Path file = files().get(0);
            byte[] written = Files.readAllBytes(file);
            assertThat(fragment).isNotEmpty();
            assertThat(sink.finishWithMetadata()).isEqualTo(fragment);
            sink.abort();
            assertThat(Files.readAllBytes(file)).isEqualTo(written);
            assertThatThrownBy(() -> sink.appendPage(rows(1)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("stopped");
            assertThat(table.currentSnapshot()).isNull();
        }
    }

    @Test
    void actualSnapshotMismatchCannotTurnFailedFinishIntoSuccessfulEmptyMetadata() throws Exception {
        Table table = table(false);
        var context = new IcebergWriteContext(table, table.schema(), "db", "table");
        try (var sink = new IcebergOverwritePageSink(context, 42)) {
            sink.appendPage(rows(1));
            assertThatThrownBy(sink::finishWithMetadata)
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Concurrent modification");
            assertThatThrownBy(sink::finishWithMetadata)
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("failure");
            sink.abort();
            assertThat(files()).isEmpty();
        }
    }

    private Table table(boolean partitioned) {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        PartitionSpec spec = partitioned ? PartitionSpec.builderFor(schema).identity("id").build()
                : PartitionSpec.unpartitioned();
        return new HadoopTables(new Configuration()).create(schema, spec, warehouse.resolve("table").toString());
    }

    private IcebergPageSink sink(Table table) {
        return new IcebergPageSink(new IcebergWriteContext(table, table.schema(), "db", "table"));
    }

    private Page rows(int count) {
        List<TypedRow> values = new ArrayList<>();
        for (long id = 0; id < count; id++) values.add(TypedRow.builder().putLong("id", id).build());
        return new Page(List.of("id"), values);
    }

    private List<Path> files() throws Exception {
        try (var paths = Files.walk(warehouse)) {
            return paths.filter(path -> path.toString().endsWith(".parquet")).toList();
        }
    }
}
