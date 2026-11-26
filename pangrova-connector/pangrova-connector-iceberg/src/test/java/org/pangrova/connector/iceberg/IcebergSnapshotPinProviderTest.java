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
import org.pangrova.common.connector.SnapshotPinTags;
import org.pangrova.connector.api.exception.SnapshotExpiredException;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinReference;

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
            String token = Long.toString(snapshot);
            var owner = new SnapshotPinOwner("index-1", 0, "attempt-1");
            pins.pinSnapshot("cat", "db", "items", token, owner);
            pins.pinSnapshot("cat", "db", "items", token, owner);
            assertThat(pins.getPinnedSnapshots("cat", "db", "items")).containsExactly(token);
            assertThat(pins.getSnapshotPinsForIndex("cat", "db", "items", "index-1"))
                    .containsExactly(new SnapshotPinReference(token, owner));
        }
        try (var reopened = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            var pins = new IcebergSnapshotPinProvider(reopened);
            String token = Long.toString(snapshot);
            var owner = new SnapshotPinOwner("index-1", 0, "attempt-1");
            assertThat(pins.getPinnedSnapshots("cat", "db", "items")).containsExactly(token);
            pins.unpinSnapshot("cat", "db", "items", Long.toString(snapshot + 1), owner);
            assertThat(pins.getPinnedSnapshots("cat", "db", "items")).containsExactly(token);
            pins.unpinSnapshot("cat", "db", "items", token, owner);
            pins.unpinSnapshot("cat", "db", "items", token, owner);
            assertThat(pins.getPinnedSnapshots("cat", "db", "items")).isEmpty();
        }
    }

    @Test
    void exactOwnerNeitherValidatesNorRemovesLegacyPin() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            catalog.createNamespace(Namespace.of("db"));
            var table = catalog.createTable(TableIdentifier.of("db", "items"), new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())));
            var file = DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/items.parquet")
                    .withFormat(FileFormat.PARQUET).withRecordCount(1).withFileSizeInBytes(32).build();
            table.newFastAppend().appendFile(file).commit();
            long snapshot = table.currentSnapshot().snapshotId();
            String token = Long.toString(snapshot);
            String legacyTag = SnapshotPinTags.indexPrefix("index-1");
            table.manageSnapshots().createTag(legacyTag, snapshot).commit();
            var pins = new IcebergSnapshotPinProvider(catalog);
            var owner = new SnapshotPinOwner("index-1", 0, "attempt-1");

            assertThatThrownBy(() -> pins.validateSnapshotPin("cat", "db", "items", token, owner))
                    .isInstanceOf(SnapshotExpiredException.class);
            pins.pinSnapshot("cat", "db", "items", token, owner);
            pins.unpinSnapshot("cat", "db", "items", token, owner);

            table.refresh();
            assertThat(table.refs().containsKey(legacyTag)).isTrue();
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
            assertThatThrownBy(() -> pins.pinSnapshot("cat", "db", "items", "42",
                    new SnapshotPinOwner("index-1", 0, "attempt-1")))
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
            var firstOwner = new SnapshotPinOwner("index-1", 0, "attempt-1");
            var secondOwner = new SnapshotPinOwner("index-1", 1, "attempt-2");
            pins.pinSnapshot("cat", "db", "items", Long.toString(first), firstOwner);

            var secondFile = DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/second.parquet")
                    .withFormat(FileFormat.PARQUET).withRecordCount(1).withFileSizeInBytes(32).build();
            table.newFastAppend().appendFile(secondFile).commit();
            long second = table.currentSnapshot().snapshotId();
            pins.pinSnapshot("cat", "db", "items", Long.toString(second), secondOwner);
            assertThat(pins.getSnapshotPinsForIndex("cat", "db", "items", "index-1"))
                    .containsExactlyInAnyOrder(
                            new SnapshotPinReference(Long.toString(first), firstOwner),
                            new SnapshotPinReference(Long.toString(second), secondOwner));

            pins.unpinSnapshot("cat", "db", "items", Long.toString(first), firstOwner);
            assertThat(pins.getSnapshotPinsForIndex("cat", "db", "items", "index-1"))
                    .containsExactly(new SnapshotPinReference(Long.toString(second), secondOwner));
        }
    }
}
