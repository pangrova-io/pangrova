package org.pangrova.shuffle.api;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Verified evidence of exchange publication. The producer digest identifies logical producer
 * partitions. It alone cannot establish that a caller-selected retry or speculative attempt won.
 * Access grants are acquired separately when binding a reader.
 */
@Experimental(reason = "Exchange publication across providers")
public record ShuffleCommitReceipt(ShufflePublication publication, int producerCount,
        String producerSetSha256, Map<Integer, Partition> partitions) {
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

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
    public record Partition(long lastSequence, long bytes, long rows, String manifestSha256) {
        public Partition {
            requireSha256(manifestSha256);
            if (lastSequence < -1 || bytes < 0 || rows < 0 || (lastSequence == -1
                    ? bytes != 0 || rows != 0 : lastSequence >= Math.min(bytes, rows))) {
                throw new IllegalArgumentException("Invalid published partition totals or sequence");
            }
        }
    }

    private static void requireSha256(String digest) {
        if (digest == null || !SHA256.matcher(digest).matches()) {
            throw new IllegalArgumentException("Expected canonical lowercase SHA-256 hex");
        }
    }
}
