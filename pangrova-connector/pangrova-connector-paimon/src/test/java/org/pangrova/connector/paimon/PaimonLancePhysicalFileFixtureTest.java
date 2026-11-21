package org.pangrova.connector.paimon;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionFileWriter;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.format.lance.LanceFileFormat;
import org.apache.paimon.format.lance.LanceFileFormatFactory;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Paimon 1.4.1 / Lance 0.39 single-file corpus; not a Dataset or Native cross-read claim. */
class PaimonLancePhysicalFileFixtureTest {
    @TempDir
    Path directory;

    @Test
    void sdkPhysicalFilePreservesNullableVectorProjectionAndPhysicalPositions() throws Exception {
        RowType type = RowType.of(DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.VECTOR(3, DataTypes.FLOAT()));
        Path file = directory.resolve("paimon-sdk-0.39.lance");
        var format = format();
        var io = new LocalFileIO();
        var location = new org.apache.paimon.fs.Path(file.toString());
        try (var writer = ((SupportsDirectWrite) format.createWriterFactory(type)).create(io, location, "")) {
            writer.addElement(GenericRow.of(Long.MIN_VALUE, BinaryString.fromString("café"),
                    BinaryVector.fromPrimitiveArray(new float[] {1, -2, 3.5f})));
            writer.addElement(GenericRow.of(7L, null, null));
            writer.addElement(GenericRow.of(Long.MAX_VALUE, BinaryString.fromString("last"),
                    BinaryVector.fromPrimitiveArray(new float[] {0, 4, -5})));
        }
        assertPhysicalFooter(file);
        var rows = read(format, type, type, io, location, null, new ArrayList<>());
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getLong(0)).isEqualTo(Long.MIN_VALUE);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("café");
        assertThat(rows.get(0).getVector(2).toFloatArray()).containsExactly(1, -2, 3.5f);
        assertThat(rows.get(1).isNullAt(1)).isTrue();
        assertThat(rows.get(1).isNullAt(2)).isTrue();
        assertThat(rows.get(2).getLong(0)).isEqualTo(Long.MAX_VALUE);
        List<Long> positions = new ArrayList<>();
        RowType projection = new RowType(List.of(type.getFields().get(0)));
        var selected = read(format, type, projection, io, location, RoaringBitmap32.bitmapOf(0, 2), positions);
        assertThat(selected).extracting(row -> row.getLong(0)).containsExactly(Long.MIN_VALUE, Long.MAX_VALUE);
        assertThat(positions).containsExactly(0L, 2L);
        assertThat(Files.exists(directory.resolve("_versions"))).isFalse();
    }

    private static LanceFileFormat format() {
        return new LanceFileFormatFactory().create(new FileFormatFactory.FormatContext(new Options(), 2, 2));
    }

    @Test
    void sdkSharedDeletionFilePreservesBothEncodingsOffsetsAndChecksums() throws Exception {
        Options options = new Options();
        options.setString("warehouse", directory.resolve("deletion-catalog").toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "deletions");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .option("bucket", "1").option("bucket-key", "id").build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            var paths = table.store().pathFactory().globalIndexFileFactory();
            var small = new BitmapDeletionVector();
            small.delete(1);
            small.delete(7);
            var wide = new Bitmap64DeletionVector();
            wide.delete(1);
            wide.delete((1L << 32) + 7);
            org.apache.paimon.index.IndexFileMeta metadata;
            try (var writer = new DeletionFileWriter(paths, table.fileIO())) {
                writer.write("small.lance", small);
                writer.write("wide.lance", wide);
                metadata = writer.result();
            }
            var location = paths.toPath(metadata);
            var localPath = location.toUri().getScheme() == null ? Path.of(location.toString())
                    : Path.of(location.toUri());
            var bytes = Files.readAllBytes(localPath);
            assertThat(metadata.fileSize()).isEqualTo(bytes.length);
            var first = metadata.dvRanges().get("small.lance");
            var second = metadata.dvRanges().get("wide.lance");
            assertThat(first.offset()).isEqualTo(1);
            assertThat(second.offset()).isEqualTo(first.offset() + first.length() + 8);
            assertSdkDeletionRange(bytes, first, false);
            assertSdkDeletionRange(bytes, second, true);
            assertThat(DeletionVector.read(table.fileIO(), new DeletionFile(location.toString(),
                    first.offset(), first.length(), first.cardinality()))).isEqualTo(small);
            assertThat(DeletionVector.read(table.fileIO(), new DeletionFile(location.toString(),
                    second.offset(), second.length(), second.cardinality()))).isEqualTo(wide);
        }
    }

    private static void assertSdkDeletionRange(byte[] bytes, org.apache.paimon.index.DeletionVectorMeta metadata,
            boolean wide) {
        int actualBytes = metadata.length() + (wide ? 0 : 8);
        var header = ByteBuffer.wrap(bytes, metadata.offset(), actualBytes).slice().order(ByteOrder.BIG_ENDIAN);
        assertThat(header.getInt()).isEqualTo(actualBytes - 8);
        header.order(wide ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
        assertThat(header.getInt()).isEqualTo(wide ? Bitmap64DeletionVector.MAGIC_NUMBER
                : BitmapDeletionVector.MAGIC_NUMBER);
        var checksum = new CRC32();
        checksum.update(bytes, metadata.offset() + 4, actualBytes - 8);
        int actualCrc = header.order(ByteOrder.BIG_ENDIAN).getInt(actualBytes - 4);
        assertThat(Integer.toUnsignedLong(actualCrc)).isEqualTo(checksum.getValue());
        assertThat(metadata.cardinality()).isEqualTo(2);
    }

    @Test
    void tableNamespaceAndCommitStayOwnedByPaimonSnapshots() throws Exception {
        Options options = new Options();
        options.setString("warehouse", directory.resolve("paimon-catalog").toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "events");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("label", DataTypes.STRING()).option("bucket", "1").option("bucket-key", "id")
                    .option("file.format", "lance").build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            var builder = table.newBatchWriteBuilder();
            try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
                writer.write(GenericRow.of(11L, BinaryString.fromString("first")));
                writer.write(GenericRow.of(12L, null));
                commit.commit(writer.prepareCommit());
            }
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1);
            var scan = table.newReadBuilder().newScan().plan();
            assertThat(scan.splits()).isNotEmpty();
            for (var split : scan.splits()) {
                var data = (DataSplit) split;
                assertThat(data.snapshotId()).isEqualTo(1);
                assertThat(data.dataFiles()).allSatisfy(file -> {
                    assertThat(file.fileFormat()).isEqualTo("lance");
                    assertThat(file.fileSize()).isPositive();
                    assertThat(file.schemaId()).isZero();
                });
            }
            var read = table.newReadBuilder();
            List<Long> ids = new ArrayList<>();
            try (var rows = read.newRead().createReader(scan).toCloseableIterator()) {
                while (rows.hasNext()) ids.add(rows.next().getLong(0));
            }
            assertThat(ids).containsExactly(11L, 12L);
        }
    }

    private static List<InternalRow> read(LanceFileFormat format, RowType source, RowType projection,
            LocalFileIO io, org.apache.paimon.fs.Path location, RoaringBitmap32 selection,
            List<Long> positions) throws Exception {
        var factory = format.createReaderFactory(source, projection, null);
        var rows = new ArrayList<InternalRow>();
        var serializer = new InternalRowSerializer(projection);
        try (var reader = factory.createReader(new FormatReaderContext(io, location, io.getFileSize(location), selection))) {
            var batch = reader.readBatch();
            while (batch != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        rows.add(serializer.copy(row));
                        positions.add(((FileRecordIterator<InternalRow>) batch).returnedPosition());
                    }
                } finally {
                    batch.releaseBatch();
                }
                batch = reader.readBatch();
            }
        }
        return rows;
    }

    private static void assertPhysicalFooter(Path file) throws Exception {
        assertThat(Files.isRegularFile(file)).isTrue();
        byte[] bytes = Files.readAllBytes(file);
        assertThat(bytes.length).isGreaterThan(8);
        var footer = ByteBuffer.wrap(bytes, bytes.length - 8, 8).order(ByteOrder.LITTLE_ENDIAN);
        // SDK 0.39's default physical writer emits 0.3, the SDK-defined wire
        // spelling of V2_0. Never rewrite the footer to the other spelling (2.0).
        assertThat(footer.getShort()).isZero();
        assertThat(footer.getShort()).isEqualTo((short) 3);
        byte[] magic = new byte[4];
        footer.get(magic);
        assertThat(magic).containsExactly((byte) 'L', (byte) 'A', (byte) 'N', (byte) 'C');
    }
}
