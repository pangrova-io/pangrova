package org.pangrova.shuffle.server;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.shuffle.codec.CanonicalShuffleReadRanges;
import org.pangrova.protocol.shuffle.codec.ShufflePartitionManifestChecksum;
import org.pangrova.protocol.v2.ShufflePartitionReadRange;
import org.pangrova.shuffle.api.ShuffleCommitReceipt.ReadRange;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static org.pangrova.shuffle.api.ShuffleCommitReceipt.MAX_READ_RANGES_PER_PARTITION;

/** Bounded certificates for a partition's absolute batch intervals. */
final class ShuffleReadRanges {
    static final long TARGET_BYTES = 64L * 1024 * 1024;
    private static final int DIGEST_BYTES = 32;
    private static final int ENTRY_BYTES = 4 * Long.BYTES + DIGEST_BYTES;

    private ShuffleReadRanges() {}

    static ShufflePartitionReadRange toProtocol(ReadRange range) {
        return ShufflePartitionReadRange.newBuilder()
                .setStartSequence(range.startSequence()).setEndSequenceExclusive(range.endSequenceExclusive())
                .setTotalBytes(range.bytes()).setTotalRows(range.rows())
                .setManifestSha256(ByteString.copyFrom(HexFormat.of().parseHex(range.manifestSha256())))
                .build();
    }

    static ReadRange fromProtocol(ShufflePartitionReadRange range) {
        if (!CanonicalShuffleReadRanges.valid(range)) {
            throw new IllegalArgumentException("shuffle read range is incomplete");
        }
        return new ReadRange(range.getStartSequence(), range.getEndSequenceExclusive(),
                range.getTotalBytes(), range.getTotalRows(),
                HexFormat.of().formatHex(range.getManifestSha256().toByteArray()));
    }

    static byte[] encode(List<ReadRange> ranges) {
        ByteBuffer out = ByteBuffer.allocate(ranges.size() * ENTRY_BYTES);
        for (ReadRange range : ranges) {
            out.putLong(range.startSequence()).putLong(range.endSequenceExclusive())
                    .putLong(range.bytes()).putLong(range.rows())
                    .put(HexFormat.of().parseHex(range.manifestSha256()));
        }
        return out.array();
    }

    static List<ReadRange> decode(byte[] encoded) {
        if (encoded.length == 0) return List.of();
        if (encoded.length % ENTRY_BYTES != 0 || encoded.length > MAX_READ_RANGES_PER_PARTITION * ENTRY_BYTES) {
            throw new IllegalStateException("invalid durable shuffle range index");
        }
        var ranges = new ArrayList<ReadRange>(encoded.length / ENTRY_BYTES);
        var input = ByteBuffer.wrap(encoded);
        byte[] digest = new byte[DIGEST_BYTES];
        while (input.hasRemaining()) {
            long start = input.getLong();
            long end = input.getLong();
            long bytes = input.getLong();
            long rows = input.getLong();
            input.get(digest);
            ranges.add(new ReadRange(start, end, bytes, rows, HexFormat.of().formatHex(digest)));
        }
        return ranges;
    }

    static final class Writer {
        private final long targetBytes;
        private final List<ReadRange> ranges = new ArrayList<>();
        private ShufflePartitionManifestChecksum.Accumulator digest;
        private long start;
        private long bytes;
        private long rows;

        Writer(long targetBytes) {
            this.targetBytes = targetBytes;
        }

        void add(long sequence, long batchBytes, long batchRows, byte[] checksum) {
            if (bytes >= targetBytes && ranges.size() < MAX_READ_RANGES_PER_PARTITION - 1) {
                seal(sequence);
                start = sequence;
                bytes = 0;
                rows = 0;
                digest = null;
            }
            if (digest == null) digest = ShufflePartitionManifestChecksum.accumulator(sequence);
            digest.add(sequence, batchBytes, batchRows, checksum);
            bytes = Math.addExact(bytes, batchBytes);
            rows = Math.addExact(rows, batchRows);
        }

        List<ReadRange> finish(long endSequence) {
            if (ranges.isEmpty()) return List.of();
            seal(endSequence);
            return List.copyOf(ranges);
        }

        private void seal(long endSequence) {
            ranges.add(new ReadRange(start, endSequence, bytes, rows,
                HexFormat.of().formatHex(digest.finishNonEmpty())));
        }
    }
}
