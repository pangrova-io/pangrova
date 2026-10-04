package org.pangrova.coordinator.task;

import com.google.protobuf.CodedOutputStream;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.spi.coordination.JournalEventContext;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.util.Objects;
import java.util.UUID;

/** Freezes task-journal identity from an already validated issued attempt and producer sequence. */
public final class TaskJournalEventContext {
    private static final byte[] DOMAIN = "pangrova.task-journal-event.v1\0".getBytes(StandardCharsets.UTF_8);

    private TaskJournalEventContext() {}

    /**
     * Retains the original producer time and issuing owner, independent of the receiving coordinator.
     *
     * @param attempt exact issued attempt previously checked by the canonical task validator
     * @param sequence frozen report sequence; zero identifies the initial scheduled fact
     * @param timestampMs original producer timestamp, reused on every delivery attempt
     * @return immutable header whose identity changes with any issued-attempt field or sequence
     */
    public static JournalEventContext create(TaskAttemptId attempt, long sequence, long timestampMs) {
        Objects.requireNonNull(attempt, "attempt");
        if (sequence < 0) throw new IllegalArgumentException("Journal report sequence must be non-negative");
        if (!attempt.hasExecutionOwner() || attempt.getExecutionOwner().getOwnerId().isBlank()) {
            throw new IllegalArgumentException("Task journal event requires its original issued owner");
        }
        byte[] hash = hash(attempt, sequence);
        // A domain-separated 128-bit content identity, not a random identity regenerated on retry.
        hash[6] = (byte) ((hash[6] & 0x0f) | 0x80);
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
        ByteBuffer bytes = ByteBuffer.wrap(hash);
        return new JournalEventContext(new UUID(bytes.getLong(), bytes.getLong()), timestampMs,
                attempt.getExecutionOwner().getOwnerId());
    }

    private static byte[] hash(TaskAttemptId attempt, long sequence) {
        var digest = DigestUtils.sha256();
        digest.update(DOMAIN);
        try (var sink = new DigestOutputStream(OutputStream.nullOutputStream(), digest)) {
            var encoded = CodedOutputStream.newInstance(sink);
            encoded.useDeterministicSerialization();
            encoded.writeUInt32NoTag(attempt.getSerializedSize());
            attempt.writeTo(encoded);
            encoded.writeUInt64NoTag(sequence);
            encoded.flush();
            return digest.digest();
        } catch (IOException failure) {
            throw new UncheckedIOException("Cannot freeze task journal event identity", failure);
        }
    }
}
