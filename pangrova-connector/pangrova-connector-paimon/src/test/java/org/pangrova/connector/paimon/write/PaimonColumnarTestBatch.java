package org.pangrova.connector.paimon.write;

import org.pangrova.protocol.batch.ColumnarBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Single-stream fixtures containing detached scalar and nested values. */
final class PaimonColumnarTestBatch {

    private PaimonColumnarTestBatch() {
    }

    static ColumnarBatch open(BufferAllocator allocator, boolean empty) throws IOException {
        byte[] bytes;
        int rows = empty ? 0 : 2;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema(), allocator);
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            root.allocateNew();
            if (!empty) {
                populate(root);
            }
            root.setRowCount(rows);
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            bytes = output.toByteArray();
        }
        return ColumnarBatch.open(ByteBuffer.wrap(bytes), allocator, rows, 5);
    }

    private static Schema schema() {
        Field item = new Field("item", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(
                field("label", ArrowType.Utf8.INSTANCE), field("quantity", new ArrowType.Int(32, true))));
        Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                new Field("key", FieldType.notNullable(new ArrowType.Int(32, true)), List.of()),
                field("value", ArrowType.Utf8.INSTANCE)));
        return new Schema(List.of(
                field("id", new ArrowType.Int(64, true)), field("name", ArrowType.Utf8.INSTANCE),
                new Field("items", FieldType.nullable(ArrowType.List.INSTANCE), List.of(item)),
                new Field("attributes", FieldType.nullable(new ArrowType.Map(false)), List.of(entries)),
                field("payload", ArrowType.Binary.INSTANCE)));
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), List.of());
    }

    private static void populate(VectorSchemaRoot root) {
        BigIntVector ids = (BigIntVector) root.getVector("id");
        ids.setSafe(0, Long.MIN_VALUE);
        ids.setSafe(1, Long.MAX_VALUE);
        VarCharVector names = (VarCharVector) root.getVector("name");
        names.setSafe(0, "café".getBytes(StandardCharsets.UTF_8));
        names.setNull(1);
        VarBinaryVector payload = (VarBinaryVector) root.getVector("payload");
        payload.setSafe(0, new byte[] {0, 1, -1});
        payload.setNull(1);
        populateItems((ListVector) root.getVector("items"));
        populateAttributes((MapVector) root.getVector("attributes"));
    }

    private static void populateItems(ListVector items) {
        StructVector values = (StructVector) items.getDataVector();
        items.startNewValue(0);
        values.setIndexDefined(0);
        values.getChild("label", VarCharVector.class).setSafe(0, "nested".getBytes(StandardCharsets.UTF_8));
        values.getChild("quantity", IntVector.class).setSafe(0, -7);
        values.setNull(1);
        items.endValue(0, 2);
        items.startNewValue(1);
        items.endValue(1, 0);
    }

    private static void populateAttributes(MapVector attributes) {
        StructVector entries = (StructVector) attributes.getDataVector();
        attributes.startNewValue(0);
        entries.setIndexDefined(0);
        entries.setIndexDefined(1);
        IntVector keys = entries.getChild("key", IntVector.class);
        keys.setSafe(0, -7);
        keys.setSafe(1, 9);
        VarCharVector values = entries.getChild("value", VarCharVector.class);
        values.setSafe(0, "typed-key".getBytes(StandardCharsets.UTF_8));
        values.setNull(1);
        attributes.endValue(0, 2);
        attributes.setNull(1);
    }
}