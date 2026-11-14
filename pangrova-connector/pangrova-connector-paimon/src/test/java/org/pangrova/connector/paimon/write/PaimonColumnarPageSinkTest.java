package org.pangrova.connector.paimon.write;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.batch.arrow.ColumnarBatch;
import org.pangrova.connector.api.data.ColumnarPageSink;
import org.pangrova.connector.api.exception.ConnectorException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises native Paimon writers using caller-owned stream batches and local storage. */
class PaimonColumnarPageSinkTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.setString("warehouse", warehouse.resolve("catalog").toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
    }

    @AfterEach
    void tearDown() throws Exception {
        catalog.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, -1})
    void writesNestedValuesAfterCallerClosesBatch(int bucket) throws Exception {
        Table table = createTable(bucket);
        try (ColumnarPageSink sink = sink(table, "APPEND"); RootAllocator allocator = new RootAllocator()) {
            try (ColumnarBatch batch = PaimonColumnarTestBatch.open(allocator, false)) {
                sink.appendColumnarBatch(batch);
                assertThat(batch.value(0, 0)).isEqualTo(Long.MIN_VALUE);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
            commit(table, sink.finishWithMetadata(), "APPEND");
        }
        List<InternalRow> rows = readRows(table);
        assertThat(rows).hasSize(2);
        assertNestedRow(rows.get(0));
        InternalRow second = rows.get(1);
        assertThat(second.getLong(0)).isEqualTo(Long.MAX_VALUE);
        assertThat(second.isNullAt(1)).isTrue();
        assertThat(second.getArray(2).size()).isZero();
        assertThat(second.isNullAt(3)).isTrue();
        assertThat(second.isNullAt(4)).isTrue();
    }

    @Test
    void recomputesOrdinalsForReorderedAndMissingColumns() throws Exception {
        Table table = createTable(1);
        try (ColumnarPageSink sink = sink(table, "APPEND"); RootAllocator allocator = new RootAllocator()) {
            try (ColumnarBatch batch = PaimonColumnarTestBatch.open(allocator, false)) {
                sink.appendColumnarBatch(batch);
                try (ColumnarBatch view = batch.project(List.of("name", "id"),
                        new int[] {1, 0}, new int[] {-1, -1})) {
                    sink.appendColumnarBatch(view);
                }
                assertThat(batch.value(0, 0)).isEqualTo(Long.MIN_VALUE);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
            commit(table, sink.finishWithMetadata(), "APPEND");
        }
        List<InternalRow> rows = readRows(table);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getLong(0)).isEqualTo(Long.MIN_VALUE);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("café");
        assertThat(rows.get(1).getLong(0)).isEqualTo(Long.MAX_VALUE);
        assertThat(rows).allSatisfy(row -> {
            assertThat(row.isNullAt(2)).isTrue();
            assertThat(row.isNullAt(3)).isTrue();
            assertThat(row.isNullAt(4)).isTrue();
            assertThat(row.isNullAt(5)).isTrue();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"APPEND", "OVERWRITE", "DELETE"})
    void emptyBatchWritesNoRows(String operation) throws Exception {
        Table table = createTable(1);
        try (ColumnarPageSink sink = sink(table, operation); RootAllocator allocator = new RootAllocator()) {
            try (ColumnarBatch batch = PaimonColumnarTestBatch.open(allocator, true)) {
                sink.appendColumnarBatch(batch);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
            commit(table, sink.finishWithMetadata(), operation);
        }
        assertThat(readRows(table)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"OVERWRITE", "DELETE"})
    void preservesOverwriteAndDeleteSemantics(String operation) throws Exception {
        Table table = createTable(1);
        try (RootAllocator allocator = new RootAllocator();
                ColumnarBatch batch = PaimonColumnarTestBatch.open(allocator, false)) {
            try (ColumnarPageSink append = sink(table, "APPEND")) {
                append.appendColumnarBatch(batch);
                commit(table, append.finishWithMetadata(), "APPEND");
            }
            try (ColumnarPageSink change = sink(table, operation);
                    ColumnarBatch keys = batch.project(List.of("id"), new int[] {0}, new int[] {-1})) {
                change.appendColumnarBatch(keys);
                commit(table, change.finishWithMetadata(), operation);
            }
        }
        List<InternalRow> rows = readRows(table);
        if (operation.equals("DELETE")) {
            assertThat(rows).isEmpty();
        } else {
            assertThat(rows).hasSize(2);
            assertThat(rows).allSatisfy(row -> assertThat(row.isNullAt(1)).isTrue());
        }
    }

    @Test
    void conversionFailureCanBeAbortedWithoutRetainingBatchOrCommittingRows() throws Exception {
        Table table = createTable(1);
        try (ColumnarPageSink sink = sink(table, "APPEND"); RootAllocator allocator = new RootAllocator()) {
            try (ColumnarBatch batch = PaimonColumnarTestBatch.open(allocator, false);
                    ColumnarBatch invalid = batch.project(List.of("id"), new int[] {1}, new int[] {-1})) {
                sink.appendColumnarBatch(batch);
                assertThatThrownBy(() -> sink.appendColumnarBatch(invalid))
                        .isInstanceOfSatisfying(ConnectorException.class,
                                error -> assertThat(error.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_WRITE_ERROR))
                        .hasCauseInstanceOf(ClassCastException.class);
                assertThat(batch.value(0, 1)).isEqualTo("café");
                sink.abort();
                assertThat(sink.finishWithMetadata()).isEmpty();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        assertThat(readRows(table)).isEmpty();
        try (var files = Files.walk(warehouse.resolve("spill"))) {
            assertThat(files.filter(Files::isRegularFile)).isEmpty();
        }
    }

    private Table createTable(int bucket) throws Exception {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("items", DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD(0, "label", DataTypes.STRING()),
                        DataTypes.FIELD(1, "quantity", DataTypes.BIGINT()))))
                .column("attributes", DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()))
                .column("payload", DataTypes.BYTES())
                .column("missing", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", Integer.toString(bucket))
                .option("file.format", "parquet")
                .build();
        Identifier identifier = Identifier.create("db", "table");
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private ColumnarPageSink sink(Table table, String operation) throws Exception {
        String spill = Files.createDirectories(warehouse.resolve("spill")).toString();
        return switch (operation) {
            case "OVERWRITE" -> new PaimonOverwritePageSink(table, "db", "table", spill, Map.of());
            case "DELETE" -> new PaimonDeletePageSink(table, "db", "table", spill);
            default -> new PaimonPageSink(table, "db", "table", spill);
        };
    }

    private void commit(Table table, byte[] metadata, String operation) throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(metadata))) {
            int version = input.readInt();
            assertThat(input.readUTF()).isEqualTo(operation);
            assertThat(input.readUTF()).isEqualTo("db");
            assertThat(input.readUTF()).isEqualTo("table");
            int count = input.readInt();
            for (int i = 0; i < count; i++) {
                messages.add(serializer.deserialize(version, input.readNBytes(input.readInt())));
            }
        }
        var builder = table.newBatchWriteBuilder();
        if (operation.equals("OVERWRITE")) {
            builder = builder.withOverwrite(Map.of());
        }
        try (var commit = builder.newCommit()) {
            commit.commit(messages);
        }
    }

    private List<InternalRow> readRows(Table table) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        var builder = table.newReadBuilder();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        try (var iterator = builder.newRead().createReader(builder.newScan().plan()).toCloseableIterator()) {
            while (iterator.hasNext()) {
                rows.add(serializer.copy(iterator.next()));
            }
        }
        rows.sort(Comparator.comparingLong(row -> row.getLong(0)));
        return rows;
    }

    private void assertNestedRow(InternalRow row) {
        assertThat(row.getLong(0)).isEqualTo(Long.MIN_VALUE);
        assertThat(row.getString(1).toString()).isEqualTo("café");
        InternalArray items = row.getArray(2);
        assertThat(items.size()).isEqualTo(2);
        assertThat(items.getRow(0, 2).getString(0).toString()).isEqualTo("nested");
        assertThat(items.getRow(0, 2).getLong(1)).isEqualTo(-7L);
        assertThat(items.isNullAt(1)).isTrue();
        InternalMap attributes = row.getMap(3);
        assertThat(attributes.size()).isEqualTo(2);
        Map<Long, String> values = new java.util.LinkedHashMap<>();
        for (int i = 0; i < attributes.size(); i++) {
            values.put(attributes.keyArray().getLong(i), attributes.valueArray().isNullAt(i)
                    ? null : attributes.valueArray().getString(i).toString());
        }
        assertThat(values).containsEntry(-7L, "typed-key").containsEntry(9L, null);
        assertThat(row.getBinary(4)).containsExactly(0, 1, -1);
        assertThat(row.isNullAt(5)).isTrue();
    }
}