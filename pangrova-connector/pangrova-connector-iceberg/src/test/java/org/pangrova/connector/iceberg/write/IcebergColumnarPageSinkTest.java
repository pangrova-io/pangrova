package org.pangrova.connector.iceberg.write;

import org.pangrova.batch.arrow.ColumnarBatch;
import org.pangrova.connector.spi.data.ColumnarPageSink;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises real Parquet sinks using caller-owned stream batches on local storage. */
class IcebergColumnarPageSinkTest {

    @TempDir
    Path warehouse;

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void writesNestedValuesAfterCallerClosesBatch(boolean partitioned, boolean overwrite) throws Exception {
        Table table = createTable(partitioned);
        byte[] metadata;
        try (ColumnarPageSink sink = sink(table, overwrite); RootAllocator allocator = new RootAllocator()) {
            try (ColumnarBatch batch = IcebergColumnarTestBatch.open(allocator, false)) {
                sink.appendColumnarBatch(batch);
                assertThat(batch.value(0, 0)).isEqualTo(Long.MIN_VALUE);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
            metadata = sink.finishWithMetadata();
        }
        List<Record> rows = readFiles(table, metadata);
        assertThat(rows).hasSize(2);
        Record first = rows.get(0);
        assertThat(first.getField("id")).isEqualTo(Long.MIN_VALUE);
        assertThat(first.getField("name")).isEqualTo("café");
        assertThat(first.getField("missing")).isNull();
        assertThat(first.getField("payload")).isEqualTo(ByteBuffer.wrap(new byte[] {0, 1, -1}));
        List<?> items = (List<?>) first.getField("items");
        assertThat(items).hasSize(2);
        assertThat(((Record) items.get(0)).getField("label")).isEqualTo("nested");
        assertThat(((Record) items.get(0)).getField("quantity")).isEqualTo(-7L);
        assertThat(items.get(1)).isNull();
        Map<?, ?> attributes = (Map<?, ?>) first.getField("attributes");
        assertThat(attributes).hasSize(2);
        assertThat(attributes.get(-7L)).isEqualTo("typed-key");
        assertThat(attributes.containsKey(9L)).isTrue();
        assertThat(attributes.get(9L)).isNull();
        Record second = rows.get(1);
        assertThat(second.getField("id")).isEqualTo(Long.MAX_VALUE);
        assertThat(second.getField("name")).isNull();
        assertThat((List<?>) second.getField("items")).isEmpty();
        assertThat(second.getField("attributes")).isNull();
        assertThat(second.getField("payload")).isNull();
    }

    @Test
    void recomputesOrdinalsForReorderedAndMissingColumns() throws Exception {
        Table table = createTable(false);
        byte[] metadata;
        try (ColumnarPageSink sink = sink(table, false); RootAllocator allocator = new RootAllocator()) {
            try (ColumnarBatch batch = IcebergColumnarTestBatch.open(allocator, false)) {
                sink.appendColumnarBatch(batch);
                try (ColumnarBatch view = batch.project(List.of("name", "id"),
                        new int[] {1, 0}, new int[] {-1, -1})) {
                    sink.appendColumnarBatch(view);
                }
                assertThat(batch.value(0, 0)).isEqualTo(Long.MIN_VALUE);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
            metadata = sink.finishWithMetadata();
        }
        List<Record> rows = readFiles(table, metadata);
        assertThat(rows).hasSize(4);
        assertThat(rows.stream().filter(row -> row.getField("items") == null)).hasSize(2);
        assertThat(rows.stream().filter(row -> "café".equals(row.getField("name"))))
                .allSatisfy(row -> assertThat(row.getField("id")).isEqualTo(Long.MIN_VALUE));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void emptyBatchWritesNoRows(boolean partitioned, boolean overwrite) throws Exception {
        Table table = createTable(partitioned);
        byte[] metadata;
        try (ColumnarPageSink sink = sink(table, overwrite); RootAllocator allocator = new RootAllocator()) {
            try (ColumnarBatch batch = IcebergColumnarTestBatch.open(allocator, true)) {
                sink.appendColumnarBatch(batch);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
            metadata = sink.finishWithMetadata();
        }
        if (overwrite) {
            // An overwrite describes the complete replacement, including an
            // empty one: an unscoped overwrite must clear the table rather
            // than leave the previous content behind.
            JsonNode commit = new ObjectMapper().readTree(metadata);
            assertThat(commit.path("operation").asText()).isEqualTo("OVERWRITE");
            assertThat(commit.path("dataFiles").isArray()).isTrue();
            assertThat(commit.path("dataFiles").size()).isZero();
        } else {
            // Empty append is a no-op.
            assertThat(metadata).isEmpty();
        }
        assertThat(readFiles(table, metadata)).isEmpty();
        try (var files = Files.walk(warehouse)) {
            assertThat(files.filter(path -> path.toString().endsWith(".parquet"))).isEmpty();
        }
    }

    @Test
    void writeFailureCanBeAbortedWithoutRetainingBatchOrFiles() throws Exception {
        Table table = createTable(false);
        try (ColumnarPageSink sink = sink(table, false); RootAllocator allocator = new RootAllocator()) {
            try (ColumnarBatch batch = IcebergColumnarTestBatch.open(allocator, false);
                    ColumnarBatch invalid = batch.project(List.of("id"), new int[] {1}, new int[] {-1})) {
                sink.appendColumnarBatch(batch);
                assertThatThrownBy(() -> sink.appendColumnarBatch(invalid)).isInstanceOf(ClassCastException.class);
                assertThat(batch.value(0, 1)).isEqualTo("café");
                sink.abort();
                assertThat(sink.finishWithMetadata()).isEmpty();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        assertThat(table.currentSnapshot()).isNull();
        try (var files = Files.walk(warehouse)) {
            assertThat(files.filter(path -> path.toString().endsWith(".parquet"))).isEmpty();
        }
    }

    private Table createTable(boolean partitioned) {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "items", Types.ListType.ofOptional(4,
                        Types.StructType.of(
                                Types.NestedField.optional(5, "label", Types.StringType.get()),
                                Types.NestedField.optional(6, "quantity", Types.LongType.get())))),
                Types.NestedField.optional(7, "attributes", Types.MapType.ofOptional(
                        8, 9, Types.LongType.get(), Types.StringType.get())),
                Types.NestedField.optional(10, "payload", Types.BinaryType.get()),
                Types.NestedField.optional(11, "missing", Types.StringType.get()));
        PartitionSpec spec = partitioned ? PartitionSpec.builderFor(schema).identity("id").build()
                : PartitionSpec.unpartitioned();
        return new HadoopTables(new Configuration()).create(schema, spec, warehouse.resolve("table").toString());
    }

    private ColumnarPageSink sink(Table table, boolean overwrite) {
        IcebergWriteContext context = new IcebergWriteContext(table, table.schema(), "db", "table");
        return overwrite ? new IcebergOverwritePageSink(context) : new IcebergPageSink(context);
    }

    private List<Record> readFiles(Table table, byte[] metadata) throws Exception {
        List<Record> rows = new ArrayList<>();
        if (metadata.length == 0) {
            return rows;
        }
        JsonNode json = new ObjectMapper().readTree(metadata);
        JsonNode files = json.isArray() ? json : json.get("dataFiles");
        for (JsonNode file : files) {
            DataFile dataFile = (DataFile) ContentFileParser.fromJson(file, table.spec());
            try (CloseableIterable<Record> records = Parquet.read(table.io().newInputFile(dataFile.location()))
                    .project(table.schema())
                    .createReaderFunc(schema -> GenericParquetReaders.buildReader(table.schema(), schema)).build()) {
                records.forEach(record -> rows.add(record.copy()));
            }
        }
        rows.sort(Comparator.comparingLong(row -> (long) row.getField("id")));
        return rows;
    }
}
