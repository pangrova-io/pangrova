package org.pangrova.protocol.batch;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Validates framing before any row can be written, without decoding buffers twice. */
final class BatchStreamValidation {
    private BatchStreamValidation() {}

    static void validate(ByteBuffer source) throws IOException {
        ByteBuffer data = source.slice().order(ByteOrder.LITTLE_ENDIAN);
        int messages = 0;
        while (data.remaining() >= 8) {
            int length = readMetadataLength(data);
            if (length == 0) {
                validateEndOfStream(data, messages);
                return;
            }
            Message message = readMetadata(data, length);
            validateHeader(message, messages);
            skipBody(data, message.bodyLength());
            messages++;
        }
        throw new IOException("Missing IPC end-of-stream marker");
    }

    private static int readMetadataLength(ByteBuffer data) throws IOException {
        if (data.getInt() != -1) {
            throw new IOException("Expected IPC continuation marker");
        }
        int length = data.getInt();
        if (length < 0 || length % 8 != 0 || length > data.remaining()) {
            throw new IOException("Invalid IPC metadata length: " + length);
        }
        return length;
    }

    private static void validateEndOfStream(ByteBuffer data, int messages) throws IOException {
        if (messages != 2 || data.hasRemaining()) {
            throw new IOException("Expected exactly one schema and one batch before end of stream");
        }
    }

    private static Message readMetadata(ByteBuffer data, int length) {
        ByteBuffer metadata = data.slice().order(ByteOrder.LITTLE_ENDIAN);
        metadata.limit(length);
        Message message = Message.getRootAsMessage(metadata);
        data.position(data.position() + length);
        return message;
    }

    private static void validateHeader(Message message, int messages) throws IOException {
        byte expected = messages == 0 ? MessageHeader.Schema : MessageHeader.RecordBatch;
        if (messages >= 2 || message.headerType() != expected) {
            throw new IOException(
                    "Expected schema then one plain record batch; dictionaries are not supported");
        }
    }

    private static void skipBody(ByteBuffer data, long bodyLength) throws IOException {
        if (bodyLength < 0 || bodyLength > data.remaining() || bodyLength % 8 != 0) {
            throw new IOException("Invalid IPC body length: " + bodyLength);
        }
        data.position(data.position() + (int) bodyLength);
    }
}
