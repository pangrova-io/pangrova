package org.pangrova.testing.e2e.write;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.stream.Stream;

/** Actual SDK stream encoding and corruption of its wire bytes, without replacing any connector. */
final class NativeWriteBatchStreams {
    private NativeWriteBatchStreams() {}

    static byte[] integers(String name, int batches, Long... values) throws IOException {
        try (var allocator = new RootAllocator(1024 * 1024)) {
            var vector = new BigIntVector(name, allocator);
            try (var root = VectorSchemaRoot.of(vector)) {
                vector.allocateNew(values.length);
                for (int row = 0; row < values.length; row++) {
                    if (values[row] == null) vector.setNull(row);
                    else vector.setSafe(row, values[row]);
                }
                vector.setValueCount(values.length);
                root.setRowCount(values.length);
                return encode(root, batches);
            }
        }
    }

    static byte[] decimals(BigDecimal... values) throws IOException {
        try (var allocator = new RootAllocator(1024 * 1024)) {
            var vector = new DecimalVector("id", allocator, 8, 2);
            try (var root = VectorSchemaRoot.of(vector)) {
                vector.allocateNew(values.length);
                for (int row = 0; row < values.length; row++) vector.setSafe(row, values[row]);
                vector.setValueCount(values.length);
                root.setRowCount(values.length);
                return encode(root, 1);
            }
        }
    }

    static byte[] ignoredAndId(long ignored, long id) throws IOException {
        try (var allocator = new RootAllocator(1024 * 1024)) {
            var first = new BigIntVector("ignored", allocator);
            var second = new BigIntVector("id", allocator);
            try (var root = VectorSchemaRoot.of(first, second)) {
                first.allocateNew(1);
                second.allocateNew(1);
                first.set(0, ignored);
                second.set(0, id);
                first.setValueCount(1);
                second.setValueCount(1);
                root.setRowCount(1);
                return encode(root, 1);
            }
        }
    }

    private static byte[] encode(VectorSchemaRoot root, int batches) throws IOException {
        try (var output = new ByteArrayOutputStream();
                var writer = new ArrowStreamWriter(root, null, output)) {
            writer.start();
            for (int batch = 0; batch < batches; batch++) writer.writeBatch();
            writer.end();
            return output.toByteArray();
        }
    }

    static ByteBuffer range(byte[] bytes, boolean direct) {
        if (bytes == null) return null;
        ByteBuffer result = direct ? ByteBuffer.allocateDirect(bytes.length + 16)
                : ByteBuffer.allocate(bytes.length + 16);
        result.position(8).put(bytes).limit(bytes.length + 8).position(8);
        return result;
    }

    static Stream<InvalidFrame> malformed() throws IOException {
        byte[] valid = integers("id", 1, 7L);
        byte[] schema = schemaFrame(valid);
        return Stream.of(new InvalidFrame("null payload", null), new InvalidFrame("empty payload", new byte[0]),
                new InvalidFrame("incomplete prefix", new byte[] {-1, -1, -1}),
                new InvalidFrame("end without schema", end()),
                new InvalidFrame("schema without batch", concatenate(schema, end())),
                new InvalidFrame("batch without schema", Arrays.copyOfRange(valid, schema.length, valid.length)),
                new InvalidFrame("second schema", concatenate(schema, valid)),
                new InvalidFrame("two record batches", integers("id", 2, 7L)),
                new InvalidFrame("two complete streams", concatenate(valid, valid)),
                new InvalidFrame("trailing byte", Arrays.copyOf(valid, valid.length + 1)),
                new InvalidFrame("missing end", Arrays.copyOf(valid, valid.length - 8)),
                new InvalidFrame("truncated end", Arrays.copyOf(valid, valid.length - 1)),
                new InvalidFrame("truncated body", Arrays.copyOf(valid, valid.length - 9)),
                new InvalidFrame("bad continuation", replaceInt(valid, 0, 0)),
                new InvalidFrame("negative metadata length", replaceInt(valid, 4, -8)),
                new InvalidFrame("unaligned metadata length", replaceInt(valid, 4, 7)),
                new InvalidFrame("oversized metadata length", replaceInt(valid, 4, Integer.MAX_VALUE - 7)),
                new InvalidFrame("corrupt metadata", replaceInt(valid, 8, Integer.MAX_VALUE)),
                new InvalidFrame("dictionary message", envelope(schema, MessageHeader.DictionaryBatch, 0)),
                new InvalidFrame("negative body length", envelope(schema, MessageHeader.RecordBatch, -8)),
                new InvalidFrame("unaligned body length", envelope(schema, MessageHeader.RecordBatch, 1)),
                new InvalidFrame("oversized body length",
                        envelope(schema, MessageHeader.RecordBatch, Long.MAX_VALUE - 7)));
    }

    static byte[] undecodable() throws IOException {
        return envelope(schemaFrame(integers("id", 1, 7L)), MessageHeader.RecordBatch, 0);
    }

    private static byte[] envelope(byte[] schema, byte type, long bodyLength) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        RecordBatch.startRecordBatch(builder);
        RecordBatch.addLength(builder, 1);
        int record = RecordBatch.endRecordBatch(builder);
        Message.startMessage(builder);
        Message.addVersion(builder, MetadataVersion.V5);
        Message.addHeaderType(builder, type);
        Message.addHeader(builder, record);
        Message.addBodyLength(builder, bodyLength);
        builder.finish(Message.endMessage(builder));
        byte[] metadata = builder.sizedByteArray();
        int paddedLength = (metadata.length + 7) & ~7;
        ByteBuffer frame = ByteBuffer.allocate(8 + paddedLength).order(ByteOrder.LITTLE_ENDIAN);
        frame.putInt(-1).putInt(paddedLength).put(metadata);
        return concatenate(schema, frame.array(), end());
    }

    private static byte[] schemaFrame(byte[] valid) {
        int length = ByteBuffer.wrap(valid).order(ByteOrder.LITTLE_ENDIAN).getInt(4);
        return Arrays.copyOf(valid, 8 + length);
    }

    private static byte[] replaceInt(byte[] bytes, int offset, int value) {
        byte[] result = bytes.clone();
        ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).putInt(offset, value);
        return result;
    }

    private static byte[] end() {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(-1).putInt(0).array();
    }

    private static byte[] concatenate(byte[]... parts) {
        ByteBuffer result = ByteBuffer.allocate(Arrays.stream(parts).mapToInt(part -> part.length).sum());
        for (byte[] part : parts) result.put(part);
        return result.array();
    }

    record InvalidFrame(String name, byte[] bytes) {
        @Override public String toString() { return name; }
    }
}
