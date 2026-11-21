package org.pangrova.worker.write;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.RecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/** Corrupt streams built from valid schema framing, without touching generated code. */
final class WriteBatchFrames {
    private WriteBatchFrames() {}

    static Stream<InvalidFrame> malformed() throws IOException {
        byte[] valid = WriteBatchFixtures.integers(List.of("id"), new Integer[] {7});
        byte[] schema = schemaFrame(valid);
        return Stream.of(
                new InvalidFrame("null payload", null),
                new InvalidFrame("empty payload", new byte[0]),
                new InvalidFrame("incomplete prefix", new byte[] {-1, -1, -1}),
                new InvalidFrame("end without schema", end()),
                new InvalidFrame("schema without batch", concatenate(schema, end())),
                new InvalidFrame("batch without schema", Arrays.copyOfRange(valid, schema.length, valid.length)),
                new InvalidFrame("second schema", concatenate(schema, valid)),
                new InvalidFrame("two record batches", WriteBatchFixtures.integerStream(
                        List.of("id"), new Integer[][] {{7}}, 2)),
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
                new InvalidFrame("oversized body length", envelope(schema, MessageHeader.RecordBatch, Long.MAX_VALUE - 7)));
    }

    static byte[] undecodable() throws IOException {
        byte[] valid = WriteBatchFixtures.integers(List.of("id"), new Integer[] {7});
        // Framing is valid, but a one-column record batch has no field nodes or buffers.
        return envelope(schemaFrame(valid), MessageHeader.RecordBatch, 0);
    }

    private static byte[] envelope(byte[] schema, byte headerType, long bodyLength) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        RecordBatch.startRecordBatch(builder);
        RecordBatch.addLength(builder, 1);
        int record = RecordBatch.endRecordBatch(builder);
        Message.startMessage(builder);
        Message.addVersion(builder, MetadataVersion.V5);
        Message.addHeaderType(builder, headerType);
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
        int metadataLength = ByteBuffer.wrap(valid).order(ByteOrder.LITTLE_ENDIAN).getInt(4);
        return Arrays.copyOf(valid, 8 + metadataLength);
    }

    private static byte[] replaceInt(byte[] data, int offset, int value) {
        byte[] changed = data.clone();
        ByteBuffer.wrap(changed).order(ByteOrder.LITTLE_ENDIAN).putInt(offset, value);
        return changed;
    }

    private static byte[] end() {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(-1).putInt(0).array();
    }

    private static byte[] concatenate(byte[]... parts) {
        ByteBuffer result = ByteBuffer.allocate(Arrays.stream(parts).mapToInt(part -> part.length).sum());
        for (byte[] part : parts) {
            result.put(part);
        }
        return result.array();
    }

    record InvalidFrame(String name, byte[] bytes) {
        @Override
        public String toString() {
            return name;
        }
    }
}