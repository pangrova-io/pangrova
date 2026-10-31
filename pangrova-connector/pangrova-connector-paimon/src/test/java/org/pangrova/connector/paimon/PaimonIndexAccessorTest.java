package org.pangrova.connector.paimon;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.spi.index.ColumnValueReader;
import org.pangrova.connector.spi.index.SnapshotDataFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the registered index accessors against real Paimon files. */
class PaimonIndexAccessorTest extends PaimonDeleteTestSupport {

    @Test
    void registeredFileSystemAccessorReadsWritesAndDeletesIndexFiles() throws Exception {
        createTable(false, true);
        var metadata = new PaimonMetadata(catalog, "index_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var support = metadata.getIndexSupport().orElseThrow();
        var accessor = support.getFileSystemAccessor().orElseThrow();
        String path = warehouse.resolve("accessor-index.bin").toUri().toString();
        byte[] bytes = "durable-index".getBytes(StandardCharsets.UTF_8);
        try (var output = accessor.openOutputStream(path)) {
            output.write(bytes);
        }
        assertThat(accessor.exists(path)).isTrue();
        assertThat(accessor.fileSize(path)).isEqualTo(bytes.length);
        try (var input = accessor.openInputStream(path)) {
            assertThat(input.readAllBytes()).containsExactly(bytes);
        }
        accessor.deleteFile(path);
        assertThat(accessor.exists(path)).isFalse();
        accessor.close();
        assertThat(catalog.listTables(DATABASE)).contains(TABLE_NAME);
    }

    @Test
    void registeredColumnReaderReadsEachPhysicalFileOnceAcrossPartitions() throws Exception {
        createTable(true, true);
        writeRows(new StoredRow(1, "east", "first", "one"),
                new StoredRow(2, "west", "first", "two"));
        writeRows(new StoredRow(3, "east", "second", "three"));
        var support = new PaimonMetadata(catalog, "index_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry())
                .getIndexSupport().orElseThrow();
        var snapshot = support.getSnapshotAwareMetadata(DATABASE, TABLE_NAME).orElseThrow();
        List<SnapshotDataFile> files = snapshot.listFilesAtSnapshot(
                snapshot.currentSnapshotId().orElseThrow());
        assertThat(files).hasSize(3);
        ColumnValueReader reader = support.getColumnValueReader().orElseThrow();
        List<Long> ids = new ArrayList<>();
        for (SnapshotDataFile file : files) {
            var rows = reader.readColumns(file.path(), List.of("id", "payload"));
            List<List<Comparable<?>>> values = new ArrayList<>();
            rows.forEachRemaining(values::add);
            assertThat(values).hasSize((int) file.rowCount());
            for (List<Comparable<?>> row : values) {
                ids.add((Long) row.get(0));
                assertThat(row.get(1)).isIn("one", "two", "three");
            }
        }
        assertThat(ids).containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    @Test
    void missingFileFailsInsteadOfReadingOtherRows() throws Exception {
        createTable(false, true);
        writeRows(new StoredRow(1, "east", "first", "one"));
        var metadata = new PaimonMetadata(catalog, "index_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var snapshot = metadata.getSnapshotAwareMetadata(DATABASE, TABLE_NAME).orElseThrow();
        String realFile = snapshot.listFilesAtSnapshot(
                snapshot.currentSnapshotId().orElseThrow()).get(0).path();
        var reader = metadata.getColumnValueReader().orElseThrow();
        assertThatThrownBy(() -> reader.readColumns(realFile + ".missing", List.of("id")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to read columns");
    }
}
