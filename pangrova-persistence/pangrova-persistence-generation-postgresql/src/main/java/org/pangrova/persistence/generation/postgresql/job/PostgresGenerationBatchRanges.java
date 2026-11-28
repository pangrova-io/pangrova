package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.generation.api.exchange.GenerationExchangeFinalization.BatchRange;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.InputRange;

import java.nio.ByteBuffer;
import java.util.HexFormat;

/** Fixed-width JDBC storage for the certificates owned by an exact physical page. */
final class PostgresGenerationBatchRanges {
    private static final int SHA_BYTES = 32;
    private static final int RANGE_BYTES = Long.BYTES * 4 + SHA_BYTES;
    private static final int FIRST = 1;
    private static final int SECOND = 2;
    private static final int BOTH = FIRST | SECOND;
    private static final byte[] WHOLE_PARTITIONS = new byte[0];

    private PostgresGenerationBatchRanges() {}

    static byte[] encode(InputRange input) {
        if (!input.split()) return WHOLE_PARTITIONS;
        int mask = (input.first() == null ? 0 : FIRST) | (input.second() == null ? 0 : SECOND);
        var output = ByteBuffer.allocate(1 + RANGE_BYTES * Integer.bitCount(mask)).put((byte) mask);
        if (input.first() != null) write(output, input.first());
        if (input.second() != null) write(output, input.second());
        return output.array();
    }

    static InputRange decode(int start, int end, byte[] certificates) {
        if (certificates == null) return new InputRange(start, end);
        if (certificates.length == 0) throw invalidCertificates();
        var input = ByteBuffer.wrap(certificates);
        int mask = input.get();
        if (mask < FIRST || mask > BOTH || input.remaining() != RANGE_BYTES * Integer.bitCount(mask)) {
            throw invalidCertificates();
        }
        var first = (mask & FIRST) == 0 ? null : read(input);
        var second = (mask & SECOND) == 0 ? null : read(input);
        return new InputRange(start, end, first, second);
    }

    private static void write(ByteBuffer output, BatchRange range) {
        output.putLong(range.startSequence()).putLong(range.endSequenceExclusive())
                .putLong(range.totalBytes()).putLong(range.totalRows())
                .put(HexFormat.of().parseHex(range.manifestSha256()));
    }

    private static BatchRange read(ByteBuffer input) {
        long start = input.getLong();
        long end = input.getLong();
        long bytes = input.getLong();
        long rows = input.getLong();
        byte[] digest = new byte[SHA_BYTES];
        input.get(digest);
        return new BatchRange(start, end, bytes, rows, HexFormat.of().formatHex(digest));
    }

    private static IllegalStateException invalidCertificates() {
        return new IllegalStateException("Frozen Shuffle page contains invalid range certificate bytes");
    }
}
