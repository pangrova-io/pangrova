package org.pangrova.transport;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArrowBatchDecoderLimitTest {
    @Test
    void boundAppliesAcrossBatchesBeforeMaterializingFurtherRows() throws Exception {
        byte[] bytes;
        try (var allocator = new RootAllocator(1024 * 1024);
                var vector = new IntVector("value", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, 42);
            vector.setSafe(1, 43);
            vector.setValueCount(2);
            try (var root = new VectorSchemaRoot(List.of(vector));
                    var output = new ByteArrayOutputStream();
                    var writer = new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
                writer.start();
                writer.writeBatch();
                writer.writeBatch();
                writer.end();
                bytes = output.toByteArray();
            }
        }
        assertThatThrownBy(() -> ArrowBatchDecoder.decode(bytes, 3, 1024 * 1024))
                .isInstanceOf(TransportException.class).hasMessageContaining("row limit");
        assertThat(ArrowBatchDecoder.decode(bytes, 4, 1024 * 1024).rows()).hasSize(4);
    }
}
