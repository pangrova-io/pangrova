package org.pangrova.connector.iceberg.write;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK files prove table configuration, explicit write schema and cleanup ownership. */
@Tag("integration")
class IcebergParquetConfigurationIntegrationTest {
    private static final Schema WRITE_SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    private static final Map<String, String> FLUSH_PROPERTIES = Map.of(
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1",
            TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1",
            TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "1",
            TableProperties.PARQUET_COMPRESSION, "gzip",
            "write.metadata.metrics.default", "none");

    @TempDir
    Path warehouse;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void configuredWriterNaturallyFlushesAndRetainsExactRowsSchemaCodecAndMetrics(boolean partitioned)
            throws Exception {
        Table table = table(partitioned, FLUSH_PROPERTIES);
        byte[] fragment;
        try (var sink = sink(table)) {
            assertThat(files()).isEmpty();
            sink.appendPage(rows(7));
            assertThat(files()).hasSize(1);
            sink.appendPage(rows(1, 8));
            assertThat(files()).hasSize(partitioned ? 3 : 1);
            fragment = sink.finishWithMetadata();
        }
        List<DataFile> dataFiles = dataFiles(table, fragment);
        assertThat(dataFiles).hasSize(partitioned ? 3 : 1);
        List<Long> actual = new ArrayList<>();
        long groups = 0;
        for (DataFile file : dataFiles) {
            assertThat(file.columnSizes()).isNullOrEmpty();
            assertThat(file.valueCounts()).isNullOrEmpty();
            assertThat(file.lowerBounds()).isNullOrEmpty();
            assertThat(file.upperBounds()).isNullOrEmpty();
            try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(file.location()), new Configuration()))) {
                var footer = reader.getFooter();
                assertThat(footer.getFileMetaData().getSchema().getFields())
                        .extracting(org.apache.parquet.schema.Type::getName).containsExactly("id");
                assertThat(footer.getBlocks()).allSatisfy(block -> {
                    assertThat(block.getRowCount()).isEqualTo(1);
                    assertThat(block.getColumns()).allSatisfy(column -> {
                        assertThat(column.getCodec()).isEqualTo(CompressionCodecName.GZIP);
                        assertThat(column.getValueCount()).isEqualTo(1);
                    });
                });
                groups += footer.getBlocks().size();
            }
            actual.addAll(read(table, file));
        }
        assertThat(groups).isEqualTo(3);
        assertThat(actual).containsExactlyInAnyOrder(7L, 1L, 8L);
        assertThat(dataFiles.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(3);
        assertThat(table.currentSnapshot()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void abortDeletesNaturallyFlushedFilesWithoutPublishingSnapshot(boolean partitioned) throws Exception {
        Table table = table(partitioned, FLUSH_PROPERTIES);
        try (var sink = sink(table)) {
            sink.appendPage(rows(7, 1));
            assertThat(files()).hasSize(partitioned ? 2 : 1);
            sink.abort();
            sink.abort();
            assertThat(files()).isEmpty();
            assertThat(sink.finishWithMetadata()).isEmpty();
            assertThat(table.currentSnapshot()).isNull();
            assertThat(table.snapshots()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void defaultWriterRemainsLazyUntilNormalFinish(boolean partitioned) throws Exception {
        Table table = table(partitioned, Map.of());
        byte[] fragment;
        try (var sink = sink(table)) {
            assertThat(files()).isEmpty();
            sink.appendPage(rows(7));
            assertThat(files()).isEmpty();
            fragment = sink.finishWithMetadata();
        }
        assertThat(files()).hasSize(1);
        List<DataFile> written = dataFiles(table, fragment);
        assertThat(written).singleElement().satisfies(file -> {
            assertThat(file.recordCount()).isEqualTo(1);
            assertThat(file.valueCounts()).containsEntry(1, 1L);
        });
        assertThat(read(table, written.get(0))).containsExactly(7L);
        assertThat(table.currentSnapshot()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidWriterConfigurationFailsWithoutDefaultFallback(boolean partitioned) throws Exception {
        Table table = table(partitioned,
                Map.of(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "0"));
        assertThatThrownBy(() -> {
            try (var sink = sink(table)) {
                sink.appendPage(rows(7));
            }
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("minimal record count");
        assertThat(files()).isEmpty();
        assertThat(table.currentSnapshot()).isNull();
    }

    private Table table(boolean partitioned, Map<String, String> properties) {
        Schema tableSchema = new Schema(WRITE_SCHEMA.findField("id"),
                Types.NestedField.optional(2, "unused", Types.StringType.get()));
        PartitionSpec spec = partitioned ? PartitionSpec.builderFor(tableSchema).identity("id").build()
                : PartitionSpec.unpartitioned();
        return new HadoopTables(new Configuration()).create(tableSchema, spec, properties,
                warehouse.resolve("table").toString());
    }

    private IcebergPageSink sink(Table table) {
        return new IcebergPageSink(new IcebergWriteContext(table, WRITE_SCHEMA, "db", "table"));
    }

    private static Page rows(long... values) {
        List<TypedRow> rows = new ArrayList<>();
        for (long value : values) {
            rows.add(TypedRow.builder().putLong("id", value).build());
        }
        return new Page(List.of("id"), rows);
    }

    private List<Path> files() throws Exception {
        try (var paths = Files.walk(warehouse)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".parquet")).toList();
        }
    }

    private static List<DataFile> dataFiles(Table table, byte[] fragment) throws Exception {
        List<DataFile> result = new ArrayList<>();
        for (var file : new ObjectMapper().readTree(fragment)) {
            result.add((DataFile) ContentFileParser.fromJson(file, table.spec()));
        }
        return result;
    }

    private static List<Long> read(Table table, DataFile file) throws Exception {
        List<Long> result = new ArrayList<>();
        try (CloseableIterable<Record> records = Parquet.read(table.io().newInputFile(file.location()))
                .project(WRITE_SCHEMA)
                .createReaderFunc(schema -> GenericParquetReaders.buildReader(WRITE_SCHEMA, schema)).build()) {
            records.forEach(record -> result.add((Long) record.getField("id")));
        }
        return result;
    }
}
