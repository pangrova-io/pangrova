package org.pangrova.batch.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.pangrova.batch.api.reader.ReadableBatch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnarBatchOwnershipTest {
    @Test
    void viewsBorrowBuffersAndNeverCloseTheirOwnerOrAllocator() throws Exception {
        try (var allocator = new RootAllocator(1024 * 1024)) {
            byte[] encoded = encode(allocator);
            try (ReadableBatch owner = ColumnarBatch.open(ByteBuffer.wrap(encoded), allocator, 2, 1)) {
                int[] ordinals = {0};
                try (ReadableBatch first = owner.project(List.of("projected"), ordinals, new int[]{-1});
                     ReadableBatch remaining = owner.project(List.of("remaining"), new int[]{0}, new int[]{-1})) {
                    ordinals[0] = -1;
                    assertThat(first.value(0, 0)).isEqualTo(7);
                    assertThat(first.value(1, 0)).isNull();
                    first.close();
                    assertThatThrownBy(() -> first.value(0, 0)).isInstanceOf(IllegalStateException.class);
                    assertThat(owner.value(0, 0)).isEqualTo(7);
                    assertThat(remaining.value(0, 0)).isEqualTo(7);
                    owner.close();
                    assertThatThrownBy(remaining::rowCount).isInstanceOf(IllegalStateException.class);
                }
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
            try (var buffer = allocator.buffer(1)) {
                assertThat(buffer.capacity()).isPositive();
            }
        }
    }

    @Test
    void rejectedDimensionsReleaseDecodedBuffers() throws Exception {
        try (var allocator = new RootAllocator(1024 * 1024)) {
            byte[] encoded = encode(allocator);
            assertThatThrownBy(() -> ColumnarBatch.open(ByteBuffer.wrap(encoded), allocator, 3, 1))
                    .isInstanceOf(IOException.class);
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private static byte[] encode(RootAllocator allocator) throws IOException {
        var schema = new Schema(List.of(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        var bytes = new ByteArrayOutputStream();
        try (var root = VectorSchemaRoot.create(schema, allocator);
             var writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
            root.allocateNew();
            IntVector values = (IntVector) root.getVector(0);
            values.setSafe(0, 7);
            values.setNull(1);
            root.setRowCount(2);
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return bytes.toByteArray();
    }
}
