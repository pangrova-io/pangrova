package org.pangrova.protocol.databuild;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.common.api.annotation.Internal;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Canonical integrity contract for immutable artifact execution bindings. */
@Internal
public final class PublishedGenerationBindingChecksum {

    private PublishedGenerationBindingChecksum() {}

    /** Signs every binding field except {@code binding_checksum} itself. */
    public static ArtifactBinding sign(ArtifactBinding binding) {
        ArtifactBinding unsigned = binding.toBuilder().clearBindingChecksum().build();
        return unsigned.toBuilder().setBindingChecksum(Checksum.newBuilder()
                .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(sha256(unsigned.toByteArray()))))
                .build();
    }

    /** Fails closed if the binding is unsigned or differs from its signed bytes. */
    public static void verify(ArtifactBinding binding) {
        if (!binding.hasBindingChecksum()
                || binding.getBindingChecksum().getAlgorithm()
                        != Checksum.Algorithm.ALGORITHM_SHA256
                || binding.getBindingChecksum().getDigest().size() != 32) {
            throw new IllegalArgumentException(
                    "artifact binding requires a SHA-256 binding checksum");
        }
        byte[] expected = binding.getBindingChecksum().getDigest().toByteArray();
        byte[] actual = sha256(binding.toBuilder()
                .clearBindingChecksum().build().toByteArray());
        if (!MessageDigest.isEqual(expected, actual)) {
            throw new IllegalArgumentException("artifact binding checksum mismatch");
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
