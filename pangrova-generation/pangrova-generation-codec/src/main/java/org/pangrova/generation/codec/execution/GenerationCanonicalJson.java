package org.pangrova.generation.codec.execution;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

/** Fixed persistence representation; bounds both serialization and replay before accepting evidence. */
final class GenerationCanonicalJson {
    private static final int MAX_BYTES = 16 * 1024 * 1024;
    private static final ObjectMapper MAPPER = JsonMapperFactory.shared()
            .addMixIn(GenerationExchangeFinalization.Partition.class, PartitionEncoding.class);
    private static final ObjectWriter WRITER = MAPPER.writer();
    private static final ObjectReader READER = MAPPER.reader();

    private GenerationCanonicalJson() {}

    private abstract static class PartitionEncoding {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        abstract String rangeIndexSha256();
    }

    static byte[] encode(Object value, String description) {
        Objects.requireNonNull(value, "value");
        var output = new BoundedOutput();
        try {
            WRITER.writeValue(output, value);
            return output.buffer.toByteArray();
        } catch (IOException failure) {
            if (output.exceeded) {
                throw new IllegalArgumentException(description + " exceeds metadata size limit", failure);
            }
            throw new IllegalStateException("Cannot encode " + description, failure);
        }
    }

    static <T> T decode(byte[] bytes, String sha256, Class<T> type, String description, String invalidBytes) {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_BYTES
                || !DigestUtils.matchesSha256(bytes, sha256)) {
            throw new IllegalArgumentException(invalidBytes);
        }
        try {
            T value = READER.forType(type).readValue(bytes);
            if (value == null || !Arrays.equals(bytes, encode(value, description))) {
                throw new IllegalArgumentException(description + " is not canonical");
            }
            return value;
        } catch (IOException failure) {
            throw new IllegalArgumentException(description + " is invalid", failure);
        }
    }

    /** Stops a large escaped/base64 envelope before buffering bytes that replay would reject. */
    private static final class BoundedOutput extends OutputStream {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private boolean exceeded;

        @Override
        public void write(int value) throws IOException {
            requireCapacity(1);
            buffer.write(value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            requireCapacity(length);
            buffer.write(bytes, offset, length);
        }

        private void requireCapacity(int length) throws IOException {
            if (length > MAX_BYTES - buffer.size()) {
                exceeded = true;
                throw new IOException("Artifact metadata exceeds its encoded byte budget");
            }
        }
    }
}
