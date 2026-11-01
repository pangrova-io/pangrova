package org.pangrova.worker.exchange;

import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.worker.exchange.transport.PageDecoder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that RemoteExchangeSource deserialization (via PageDecoder)
 * correctly converts Arrow IPC stream bytes into complete typed Page instances.
 *
 * <p><b>Validates: Requirements 1.56, 2.56</b></p>
 */
class RemoteExchangeSourceDeserializationTest {

    private BufferAllocator rootAllocator;
    private BufferAllocator writeAllocator;
    private BufferAllocator readAllocator;

    @BeforeEach
    void setUp() {
        rootAllocator = new RootAllocator(Long.MAX_VALUE);
        writeAllocator = rootAllocator.newChildAllocator("write", 0, Long.MAX_VALUE);
        readAllocator = rootAllocator.newChildAllocator("read", 0, Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        readAllocator.close();
        writeAllocator.close();
        rootAllocator.close();
    }

    @Test
    void deserializePage_returnsCompleteIntData() throws IOException {
        Schema schema = new Schema(List.of(
                Field.nullable("id", new ArrowType.Int(32, true))));

        byte[] batchData = createBatchData(schema, root -> {
            IntVector vec = (IntVector) root.getVector("id");
            vec.allocateNew(3);
            vec.set(0, 100);
            vec.set(1, 200);
            vec.set(2, 300);
            vec.setValueCount(3);
            root.setRowCount(3);
        });

        Page page = PageDecoder.decode(batchData, readAllocator);

        assertThat(page.columns()).containsExactly("id");
        assertThat(page.getPositionCount()).isEqualTo(3);
        assertThat(page.rows().get(0).getInt("id")).isEqualTo(100);
        assertThat(page.rows().get(1).getInt("id")).isEqualTo(200);
        assertThat(page.rows().get(2).getInt("id")).isEqualTo(300);
    }

    @Test
    void deserializePage_returnsCompleteStringData() throws IOException {
        Schema schema = new Schema(List.of(
                Field.nullable("name", new ArrowType.Utf8())));

        byte[] batchData = createBatchData(schema, root -> {
            VarCharVector vec = (VarCharVector) root.getVector("name");
            vec.allocateNew();
            vec.set(0, "alice".getBytes(StandardCharsets.UTF_8));
            vec.set(1, "bob".getBytes(StandardCharsets.UTF_8));
            vec.setValueCount(2);
            root.setRowCount(2);
        });

        Page page = PageDecoder.decode(batchData, readAllocator);

        assertThat(page.columns()).containsExactly("name");
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(page.rows().get(0).getString("name")).isEqualTo("alice");
        assertThat(page.rows().get(1).getString("name")).isEqualTo("bob");
    }

    @Test
    void deserializePage_returnsMultiColumnData() throws IOException {
        Schema schema = new Schema(List.of(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("name", new ArrowType.Utf8()),
                Field.nullable("score", new ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));

        byte[] batchData = createBatchData(schema, root -> {
            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            Float8Vector scoreVec = (Float8Vector) root.getVector("score");

            idVec.allocateNew(2);
            nameVec.allocateNew();
            scoreVec.allocateNew(2);

            idVec.set(0, 1);
            nameVec.set(0, "alice".getBytes(StandardCharsets.UTF_8));
            scoreVec.set(0, 95.5);

            idVec.set(1, 2);
            nameVec.set(1, "bob".getBytes(StandardCharsets.UTF_8));
            scoreVec.set(1, 87.3);

            idVec.setValueCount(2);
            nameVec.setValueCount(2);
            scoreVec.setValueCount(2);
            root.setRowCount(2);
        });

        Page page = PageDecoder.decode(batchData, readAllocator);

        assertThat(page.columns()).containsExactly("id", "name", "score");
        assertThat(page.getPositionCount()).isEqualTo(2);

        TypedRow row0 = page.rows().get(0);
        assertThat(row0.getInt("id")).isEqualTo(1);
        assertThat(row0.getString("name")).isEqualTo("alice");
        assertThat(row0.getDouble("score")).isEqualTo(95.5);

        TypedRow row1 = page.rows().get(1);
        assertThat(row1.getInt("id")).isEqualTo(2);
        assertThat(row1.getString("name")).isEqualTo("bob");
        assertThat(row1.getDouble("score")).isEqualTo(87.3);
    }

    @Test
    void deserializePage_handlesNullValues() throws IOException {
        Schema schema = new Schema(List.of(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("name", new ArrowType.Utf8())));

        byte[] batchData = createBatchData(schema, root -> {
            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");

            idVec.allocateNew(2);
            nameVec.allocateNew();

            idVec.set(0, 42);
            nameVec.setNull(0);

            idVec.setNull(1);
            nameVec.set(1, "present".getBytes(StandardCharsets.UTF_8));

            idVec.setValueCount(2);
            nameVec.setValueCount(2);
            root.setRowCount(2);
        });

        Page page = PageDecoder.decode(batchData, readAllocator);

        assertThat(page.getPositionCount()).isEqualTo(2);

        TypedRow row0 = page.rows().get(0);
        assertThat(row0.getInt("id")).isEqualTo(42);
        assertThat(row0.get("name")).isInstanceOf(ColumnValue.NullValue.class);

        TypedRow row1 = page.rows().get(1);
        assertThat(row1.get("id")).isInstanceOf(ColumnValue.NullValue.class);
        assertThat(row1.getString("name")).isEqualTo("present");
    }

    @Test
    void deserializePage_handlesBooleanAndLongTypes() throws IOException {
        Schema schema = new Schema(List.of(
                Field.nullable("active", new ArrowType.Bool()),
                Field.nullable("count", new ArrowType.Int(64, true))));

        byte[] batchData = createBatchData(schema, root -> {
            BitVector activeVec = (BitVector) root.getVector("active");
            BigIntVector countVec = (BigIntVector) root.getVector("count");

            activeVec.allocateNew(2);
            countVec.allocateNew(2);

            activeVec.set(0, 1);
            countVec.set(0, 999_999_999L);

            activeVec.set(1, 0);
            countVec.set(1, 123_456_789L);

            activeVec.setValueCount(2);
            countVec.setValueCount(2);
            root.setRowCount(2);
        });

        Page page = PageDecoder.decode(batchData, readAllocator);

        assertThat(page.columns()).containsExactly("active", "count");
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(page.rows().get(0).getBoolean("active")).isTrue();
        assertThat(page.rows().get(0).getLong("count")).isEqualTo(999_999_999L);
        assertThat(page.rows().get(1).getBoolean("active")).isFalse();
        assertThat(page.rows().get(1).getLong("count")).isEqualTo(123_456_789L);
    }

    @Test
    void deserializePage_emptyBytesReturnsEmptyPage() throws IOException {
        Page page = PageDecoder.decode(new byte[0], readAllocator);
        assertThat(page.isEmpty()).isTrue();
        assertThat(page.columns()).isEmpty();
    }

    @Test
    void deserializePage_nullBytesReturnsEmptyPage() throws IOException {
        Page page = PageDecoder.decode(null, readAllocator);
        assertThat(page.isEmpty()).isTrue();
        assertThat(page.columns()).isEmpty();
    }

    /**
     * Creates Arrow IPC stream bytes from a schema and data populator.
     */
    private byte[] createBatchData(Schema schema,
                                  java.util.function.Consumer<VectorSchemaRoot> populator)
            throws IOException {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, writeAllocator)) {
            populator.accept(root);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                    root, null, Channels.newChannel(baos))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }
}
