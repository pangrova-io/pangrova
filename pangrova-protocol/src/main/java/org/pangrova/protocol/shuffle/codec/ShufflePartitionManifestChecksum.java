package org.pangrova.protocol.shuffle.codec;

import org.pangrova.common.util.DigestUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Canonical digest for one ordered producer or finalized shuffle partition manifest. */
public final class ShufflePartitionManifestChecksum {

    private static final int SHA256_BYTES = 32;
    private static final byte[] DOMAIN = (
            "pangrova.shuffle.partition-manifest.v2\0").getBytes(StandardCharsets.UTF_8);

    private ShufflePartitionManifestChecksum() {}

    /** Creates a fresh accumulator whose first accepted batch sequence is zero. */
    public static Accumulator accumulator() {
        return new Accumulator(DigestUtils.sha256());
    }

    /** Mutable single-owner accumulator for fixed-width canonical manifest entries. */
    public static final class Accumulator {
        private final MessageDigest digest;
        private final ByteBuffer encodedEntry = ByteBuffer.allocate(Long.BYTES * 3).order(ByteOrder.BIG_ENDIAN);
        private long nextSequence;
        private boolean finished;

        private Accumulator(MessageDigest digest) {
            this.digest = digest;
            digest.update(DOMAIN);
        }

        /** Adds the next complete batch metadata in exact sequence order. */
        public Accumulator add(
                long sequence,
                long sizeBytes,
                long rowCount,
                byte[] payloadSha256) {
            requireEntry(sequence, sizeBytes, rowCount, payloadSha256);
            encodedEntry.clear().putLong(sequence).putLong(sizeBytes).putLong(rowCount);
            digest.update(encodedEntry.array());
            digest.update(payloadSha256);
            nextSequence++;
            return this;
        }

        /** Finishes a manifest, including the canonical empty partition, exactly once. */
        public byte[] finish() {
            if (finished) {
                throw new IllegalStateException("partition manifest may only be finished once");
            }
            finished = true;
            return digest.digest();
        }

        /** Finishes exactly once and rejects an empty producer partition manifest. */
        public byte[] finishNonEmpty() {
            if (nextSequence == 0) {
                throw new IllegalStateException("producer partition manifest must not be empty");
            }
            return finish();
        }

        private void requireEntry(
                long sequence,
                long sizeBytes,
                long rowCount,
                byte[] payloadSha256) {
            if (finished || sequence != nextSequence) {
                throw new IllegalArgumentException("manifest batch sequence is not contiguous");
            }
            if (sizeBytes <= 0 || rowCount <= 0
                    || payloadSha256 == null || payloadSha256.length != SHA256_BYTES) {
                throw new IllegalArgumentException("manifest batch metadata is invalid");
            }
        }
    }
}
