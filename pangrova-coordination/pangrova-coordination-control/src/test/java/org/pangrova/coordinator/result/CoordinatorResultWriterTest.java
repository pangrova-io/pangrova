package org.pangrova.coordinator.result;

import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.result.CoordinatorResultTestAccess.streamedRoot;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the actual Arrow vectors and their allocation before Flight sends them.
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
    void binarySnapshotsSurviveArrowFillAfterCallerMutation() throws Exception {
        byte[] source = {(byte) 0xff, 0, (byte) 0x80};
        var row = TypedRow.builder().putRaw("payload", source).build();
        source[0] = 7;
        ((byte[]) row.getRaw("payload"))[1] = 7;

        try (var batch = writer.buildBatch(List.of(row), List.of("payload"))) {
            assertThat((byte[]) streamedRoot(batch::streamTo).getVector("payload").getObject(0))
                    .containsExactly((byte) 0xff, (byte) 0, (byte) 0x80);
        }
    }

    @Test
    void typedRowsRoundTripPreservesIntAndStringValues() throws Exception {
        List<TypedRow> rows = List.of(
                TypedRow.builder().putInt("id", 1).putString("name", "alice").build(),
                TypedRow.builder().putInt("id", 2).putString("name", "bob").build());
        List<String> columns = List.of("id", "name");

        try (var batch = writer.buildBatch(rows, columns)) {
            VectorSchemaRoot root = streamedRoot(batch::streamTo);
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

        try (var batch = writer.buildBatch(rows, columns)) {
            VectorSchemaRoot root = streamedRoot(batch::streamTo);
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

        try (var batch = writer.buildBatch(rows, columns)) {
            VectorSchemaRoot root = streamedRoot(batch::streamTo);
            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(root.getVector("name").getObject(0).toString()).isEqualTo("alice");
            assertThat(root.getVector("age").isNull(0)).isTrue();
        }
    }

    @Test
    void emptyRowListRetainsItsArrowSchema() throws Exception {
        List<TypedRow> rows = List.of();
        List<String> columns = List.of("col1");

        try (var batch = writer.buildBatch(rows, columns)) {
            assertThat(streamedRoot(batch::streamTo).getRowCount()).isEqualTo(0);
            assertThat(streamedRoot(batch::streamTo).getSchema().getFields()).extracting(field -> field.getName())
                    .containsExactly("col1");
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
        assertThatThrownBy(() -> writer.buildBatch(null, List.of("a")))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> writer.buildBatch(List.of(), null))
                .isInstanceOf(NullPointerException.class);
    }
}
