package org.pangrova.shuffle.api;

import org.pangrova.common.crypto.Sha256Digests;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Verified evidence of exchange publication. The producer digest identifies logical producer
 * partitions. It alone cannot establish that a caller-selected retry or speculative attempt won.
 * Access grants are acquired separately when binding a reader.
 */
@Experimental(reason = "Exchange publication across providers")
public record ShuffleCommitReceipt(ShufflePublication publication, int producerCount,
        String producerSetSha256, Map<Integer, Partition> partitions) {
    public static final int MAX_READ_RANGES_PER_PARTITION = 32;

    public ShuffleCommitReceipt {
        Objects.requireNonNull(publication, "publication");
        requireSha256(producerSetSha256);
        partitions = Map.copyOf(partitions);
        if (producerCount <= 0 || partitions.isEmpty()) {
            throw new IllegalArgumentException("Publication requires producers and output partitions");
        }
        for (int id = 0; id < partitions.size(); id++) {
            if (!partitions.containsKey(id)) {
                throw new IllegalArgumentException("Missing output partition " + id);
            }
        }
    }

    /** Verified boundary and content digest of one output, including an explicitly empty output. */
    public record Partition(long lastSequence, long bytes, long rows, String manifestSha256,
                            List<ReadRange> readRanges) {
        public Partition(long lastSequence, long bytes, long rows, String manifestSha256) {
            this(lastSequence, bytes, rows, manifestSha256, List.of());
        }

        public Partition {
            requireSha256(manifestSha256);
            if (lastSequence < -1 || bytes < 0 || rows < 0 || (lastSequence == -1
                    ? bytes != 0 || rows != 0 : lastSequence >= Math.min(bytes, rows))) {
                throw new IllegalArgumentException("Invalid published partition totals or sequence");
            }
            readRanges = List.copyOf(readRanges);
            requireRangeCoverage(lastSequence, bytes, rows, readRanges);
        }
    }

    /** Published checksum and exact absolute coordinates of one nonempty batch interval. */
    public record ReadRange(long startSequence, long endSequenceExclusive, long bytes, long rows,
                            String manifestSha256) {
        public ReadRange {
            requireSha256(manifestSha256);
            if (startSequence < 0 || endSequenceExclusive <= startSequence
                    || endSequenceExclusive - startSequence > Math.min(bytes, rows)) {
                throw new IllegalArgumentException("Invalid published shuffle read range");
            }
        }
    }

    /** A nonempty range index must partition the original ordered output exactly once. */
    public static void requireRangeCoverage(long lastSequence, long bytes, long rows, List<ReadRange> ranges) {
        if (ranges.isEmpty()) return;
        if (ranges.size() < 2 || ranges.size() > MAX_READ_RANGES_PER_PARTITION) {
            throw new IllegalArgumentException("Invalid shuffle read range count");
        }
        long next = 0;
        long rangeBytes = 0;
        long rangeRows = 0;
        for (ReadRange range : ranges) {
            if (range.startSequence() != next) {
                throw new IllegalArgumentException("Shuffle read ranges do not cover contiguous sequences");
            }
            next = range.endSequenceExclusive();
            rangeBytes = Math.addExact(rangeBytes, range.bytes());
            rangeRows = Math.addExact(rangeRows, range.rows());
        }
        if (next - 1 != lastSequence || rangeBytes != bytes || rangeRows != rows) {
            throw new IllegalArgumentException("Shuffle read ranges do not cover the partition totals");
        }
    }

    private static void requireSha256(String digest) {
        if (!Sha256Digests.isSha256Hex(digest)) {
            throw new IllegalArgumentException("Expected canonical lowercase SHA-256 hex");
        }
    }
}
