package org.pangrova.generation.api.exchange;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Compact, capability-free receipt for a published exchange and the digest of the exact provider response. */
public record GenerationExchangeFinalization(
        String exchangeId, String registrationSha256, String shuffleId, long planEpoch,
        GenerationExecutionOwner finalizingOwner, int producerCount, String producerSetSha256,
        List<Partition> partitions, long totalBytes, long totalRows,
        String responseSha256) {
    public GenerationExchangeFinalization {
        GenerationExchangeRegistration.requireText(exchangeId);
        GenerationExchangeRegistration.requireText(shuffleId);
        GenerationExchangeRegistration.requireSha256(registrationSha256);
        GenerationExchangeRegistration.requireSha256(producerSetSha256);
        GenerationExchangeRegistration.requireSha256(responseSha256);
        Objects.requireNonNull(finalizingOwner, "finalizingOwner");
        if (planEpoch <= 0 || producerCount <= 0 || totalBytes < 0 || totalRows < 0) {
            throw new IllegalArgumentException("Invalid external finalize identity or totals");
        }
        partitions = partitions.stream().sorted(Comparator.comparingInt(Partition::partitionId)).toList();
        long bytes = 0;
        long rows = 0;
        for (int i = 0; i < partitions.size(); i++) {
            var partition = partitions.get(i);
            if (partition.partitionId() != i) {
                throw new IllegalArgumentException("Finalize partition cohort is not exact");
            }
            bytes = Math.addExact(bytes, partition.totalBytes());
            rows = Math.addExact(rows, partition.totalRows());
        }
        if (partitions.isEmpty() || bytes != totalBytes || rows != totalRows) {
            throw new IllegalArgumentException("Finalize partition totals are incomplete or inconsistent");
        }
    }

    public record Partition(int partitionId, long totalBytes, long totalRows,
            long committedLastSequence, String manifestSha256, String rangeIndexSha256) {
        public Partition(int partitionId, long totalBytes, long totalRows,
                long committedLastSequence, String manifestSha256) {
            this(partitionId, totalBytes, totalRows, committedLastSequence, manifestSha256, null);
        }

        public Partition {
            GenerationExchangeRegistration.requireSha256(manifestSha256);
            if (rangeIndexSha256 != null) GenerationExchangeRegistration.requireSha256(rangeIndexSha256);
            if (partitionId < 0 || totalBytes < 0 || totalRows < 0 || committedLastSequence < -1) {
                throw new IllegalArgumentException("Invalid finalized partition");
            }
        }

        /** Projects the verified provider receipt into the compact durable authority. */
        public static Partition fromPublication(int partitionId, ShuffleCommitReceipt.Partition published) {
            var ranges = published.readRanges().stream().map(range -> new BatchRange(
                    range.startSequence(), range.endSequenceExclusive(), range.bytes(), range.rows(),
                    range.manifestSha256())).toList();
            String index = ranges.isEmpty() ? null : HexFormat.of().formatHex(rangeDigest(ranges));
            return new Partition(partitionId, published.bytes(), published.rows(), published.lastSequence(),
                    published.manifestSha256(), index);
        }

        /** Requires the complete certified index for a split input, using only its compact commitment. */
        public void requireRangeIndex(List<BatchRange> ranges) {
            if (ranges.size() < 2 || ranges.size() > MAX_BATCH_RANGES || rangeIndexSha256 == null
                    || !MessageDigest.isEqual(rangeDigest(ranges), HexFormat.of().parseHex(rangeIndexSha256))) {
                throw new IllegalArgumentException("Proposed batch ranges differ from the finalized range index");
            }
            long next = 0;
            long bytes = 0;
            long rows = 0;
            for (var range : ranges) {
                if (range.startSequence() != next) {
                    throw new IllegalArgumentException("Finalized batch ranges have a gap or overlap");
                }
                next = range.endSequenceExclusive();
                bytes = Math.addExact(bytes, range.totalBytes());
                rows = Math.addExact(rows, range.totalRows());
            }
            if (next - 1 != committedLastSequence || bytes != totalBytes || rows != totalRows) {
                throw new IllegalArgumentException("Finalized batch ranges change the partition totals");
            }
        }
    }

    public static final int MAX_BATCH_RANGES = ShuffleCommitReceipt.MAX_READ_RANGES_PER_PARTITION;
    private static final byte[] RANGE_INDEX_DOMAIN =
            "pangrova.generation.exchange.range-index.v1\0".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    /** Bounds expansion while allowing one fully split bucket in a narrow stage. */
    public static long maximumAdaptiveTasks(int originalCount) {
        return (long) originalCount + Math.max(originalCount, MAX_BATCH_RANGES - 1);
    }

    private static byte[] rangeDigest(List<BatchRange> ranges) {
        var digest = DigestUtils.sha256();
        digest.update(RANGE_INDEX_DOMAIN);
        var coordinates = ByteBuffer.allocate(4 * Long.BYTES);
        var hex = HexFormat.of();
        for (var range : ranges) {
            coordinates.clear().putLong(range.startSequence()).putLong(range.endSequenceExclusive())
                    .putLong(range.totalBytes()).putLong(range.totalRows());
            digest.update(coordinates.array());
            digest.update(hex.parseHex(range.manifestSha256()));
        }
        return digest.digest();
    }

    /** Exact certificate for an independently bounded batch stream inside a finalized partition. */
    public record BatchRange(long startSequence, long endSequenceExclusive,
            long totalBytes, long totalRows, String manifestSha256) {
        public BatchRange {
            GenerationExchangeRegistration.requireSha256(manifestSha256);
            if (startSequence < 0 || endSequenceExclusive <= startSequence
                    || endSequenceExclusive - startSequence > Math.min(totalBytes, totalRows)) {
                throw new IllegalArgumentException("Invalid certified batch range");
            }
        }
    }
}
