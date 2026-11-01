package org.pangrova.coordinator.result;

import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for CoordinatorResultWriter — TypedRow→Arrow IPC round-trip.
 * Uses real Arrow allocator and IPC reader to verify serialization correctness.
 */
class CoordinatorResultWriterTest {

    private BufferAllocator allocator;
    private CoordinatorResultWriter writer;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(64 * 1024 * 1024);
        writer = new CoordinatorResultWriter(allocator);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void typedRowsRoundTripPreservesIntAndStringValues() throws Exception {
        List<TypedRow> rows = List.of(
                TypedRow.builder().putInt("id", 1).putString("name", "alice").build(),
                TypedRow.builder().putInt("id", 2).putString("name", "bob").build());
        List<String> columns = List.of("id", "name");

        byte[] ipc = writer.serializeTypedRows(rows, columns);

        assertThat(ipc).isNotEmpty();
        try (ArrowStreamReader reader = newReader(ipc)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(2);
            assertThat(root.getSchema().getFields()).hasSize(2);
            assertThat(root.getSchema().getFields().get(0).getName()).isEqualTo("id");
            assertThat(root.getSchema().getFields().get(1).getName()).isEqualTo("name");
            assertThat(root.getVector("id").getObject(0)).isEqualTo(1);
            assertThat(root.getVector("id").getObject(1)).isEqualTo(2);
            assertThat(root.getVector("name").getObject(0).toString()).isEqualTo("alice");
            assertThat(root.getVector("name").getObject(1).toString()).isEqualTo("bob");
        }
    }

    @Test
    void typedRowsRoundTripPreservesLongAndBooleanValues() throws Exception {
        List<TypedRow> rows = List.of(
                TypedRow.builder()
                        .putLong("count", 999_999_999_999L)
                        .put("active", new ColumnValue.BooleanValue(true))
                        .build());
        List<String> columns = List.of("count", "active");

        byte[] ipc = writer.serializeTypedRows(rows, columns);

        try (ArrowStreamReader reader = newReader(ipc)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(root.getVector("count").getObject(0)).isEqualTo(999_999_999_999L);
            assertThat(root.getVector("active").getObject(0)).isEqualTo(true);
        }
    }

    @Test
    void typedRowsHandlesNullValues() throws Exception {
        List<TypedRow> rows = List.of(
                TypedRow.builder()
                        .putString("name", "alice")
                        .put("age", new ColumnValue.NullValue())
                        .build());
        List<String> columns = List.of("name", "age");

        byte[] ipc = writer.serializeTypedRows(rows, columns);

        try (ArrowStreamReader reader = newReader(ipc)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(root.getVector("name").getObject(0).toString()).isEqualTo("alice");
            assertThat(root.getVector("age").isNull(0)).isTrue();
        }
    }

    @Test
    void emptyRowListProducesValidIpcWithZeroRows() throws Exception {
        List<TypedRow> rows = List.of();
        List<String> columns = List.of("col1");

        byte[] ipc = writer.serializeTypedRows(rows, columns);

        assertThat(ipc).isNotEmpty();
        try (ArrowStreamReader reader = newReader(ipc)) {
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(0);
        }
    }

    @Test
    void constructorRejectsNullAllocator() {
        assertThatThrownBy(() -> new CoordinatorResultWriter(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("allocator");
    }

    @Test
    void serializeTypedRowsRejectsNullArguments() {
        assertThatThrownBy(() -> writer.serializeTypedRows(null, List.of("a")))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> writer.serializeTypedRows(List.of(), null))
                .isInstanceOf(NullPointerException.class);
    }


    private ArrowStreamReader newReader(byte[] ipc) {
        return new ArrowStreamReader(
                Channels.newChannel(new ByteArrayInputStream(ipc)), allocator);
    }
}
