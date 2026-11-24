package org.pangrova.shuffle.checksum;

import org.pangrova.common.util.DigestUtils;

import java.security.MessageDigest;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.Checksum;

/** Canonical SHA-256 operations used by remote shuffle storage and protocol checks. */
public final class ShuffleChecksums {

    private static final int SHA256_BYTES = 32;

    private ShuffleChecksums() {}

    /** Returns the SHA-256 digest of the supplied bytes. */
    public static byte[] sha256(byte[] data) {
        return sha256Digest().digest(data);
    }

    /** Hashes a byte range directly from its immutable owner. */
    public static byte[] sha256(byte[] data, int offset, int length) {
        MessageDigest digest = sha256Digest();
        digest.update(data, offset, length);
        return digest.digest();
    }

    /** Hashes immutable Protobuf buffers without flattening the payload. */
    public static byte[] sha256(ByteString data) {
        MessageDigest digest = sha256Digest();
        data.asReadOnlyByteBufferList().forEach(digest::update);
        return digest.digest();
    }

    static MessageDigest sha256Digest() {
        return DigestUtils.sha256();
    }

    /** Validates and extracts one canonical SHA-256 protocol checksum. */
    public static byte[] requireSha256(Checksum checksum, String subject) {
        if (checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != SHA256_BYTES) {
            throw new IllegalArgumentException(subject + " must use a 32-byte SHA-256 digest");
        }
        return checksum.getDigest().toByteArray();
    }

    /** Encodes a 32-byte SHA-256 digest in the canonical protocol type. */
    public static Checksum protocol(byte[] digest) {
        if (digest.length != SHA256_BYTES) {
            throw new IllegalArgumentException("SHA-256 digest must contain 32 bytes");
        }
        return Checksum.newBuilder()
                .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(com.google.protobuf.ByteString.copyFrom(digest))
                .build();
    }
}
