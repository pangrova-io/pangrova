package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.spi.index.SnapshotExpiredException;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergSnapshotPinProviderTest {
    @TempDir Path warehouse;

    @Test
    void tagSurvivesCatalogRestartAndUnpinsOnlyExactSnapshot() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        var id = TableIdentifier.of("db", "items");
        long snapshot;
        try (var first = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            first.createNamespace(Namespace.of("db"));
            var table = first.createTable(id, new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())));
            var file = DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/items.parquet")
                    .withFormat(FileFormat.PARQUET).withRecordCount(1).withFileSizeInBytes(32).build();
            table.newFastAppend().appendFile(file).commit();
            snapshot = table.currentSnapshot().snapshotId();
            var pins = new IcebergSnapshotPinProvider(first);
            pins.pinSnapshot("cat", "db", "items", snapshot, "index-1");
            pins.pinSnapshot("cat", "db", "items", snapshot, "index-1");
            assertThat(pins.getPinnedSnapshots("cat", "db", "items")).containsExactly(snapshot);
            assertThat(pins.getPinnedSnapshotsForIndex("cat", "db", "items", "index-1"))
                    .containsExactly(snapshot);
        }
        try (var reopened = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            var pins = new IcebergSnapshotPinProvider(reopened);
            assertThat(pins.getPinnedSnapshots("cat", "db", "items")).containsExactly(snapshot);
            pins.unpinSnapshot("cat", "db", "items", snapshot + 1, "index-1");
            assertThat(pins.getPinnedSnapshots("cat", "db", "items")).containsExactly(snapshot);
            pins.unpinSnapshot("cat", "db", "items", snapshot, "index-1");
            pins.unpinSnapshot("cat", "db", "items", snapshot, "index-1");
            assertThat(pins.getPinnedSnapshots("cat", "db", "items")).isEmpty();
        }
    }

    @Test
    void absentSnapshotCannotBePinned() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            catalog.createNamespace(Namespace.of("db"));
            catalog.createTable(TableIdentifier.of("db", "items"), new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())));
            var pins = new IcebergSnapshotPinProvider(catalog);
            assertThatThrownBy(() -> pins.pinSnapshot("cat", "db", "items", 42, "index-1"))
                    .isInstanceOf(SnapshotExpiredException.class);
        }
    }

    @Test
    void replacementSnapshotKeepsOldRefUntilItsOwnRelease() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            catalog.createNamespace(Namespace.of("db"));
            var table = catalog.createTable(TableIdentifier.of("db", "items"), new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())));
            var firstFile = DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/first.parquet")
                    .withFormat(FileFormat.PARQUET).withRecordCount(1).withFileSizeInBytes(32).build();
            table.newFastAppend().appendFile(firstFile).commit();
            long first = table.currentSnapshot().snapshotId();
            var pins = new IcebergSnapshotPinProvider(catalog);
            pins.pinSnapshot("cat", "db", "items", first, "index-1");

            var secondFile = DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/second.parquet")
                    .withFormat(FileFormat.PARQUET).withRecordCount(1).withFileSizeInBytes(32).build();
            table.newFastAppend().appendFile(secondFile).commit();
            long second = table.currentSnapshot().snapshotId();
            pins.pinSnapshot("cat", "db", "items", second, "index-1");
            assertThat(pins.getPinnedSnapshotsForIndex("cat", "db", "items", "index-1"))
                    .containsExactlyInAnyOrder(first, second);

            pins.unpinSnapshot("cat", "db", "items", first, "index-1");
            assertThat(pins.getPinnedSnapshotsForIndex("cat", "db", "items", "index-1"))
                    .containsExactly(second);
        }
    }
}
