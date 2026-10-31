package org.pangrova.ai.worker.inference.provider.openai;

import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.embedding.BlobReference;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.function.Function;

/** Loads one governed FILE and verifies its immutable content identity. */
final class GovernedBlobContent {

    private GovernedBlobContent() {}

    static byte[] loadVerified(
            Function<BlobReference, byte[]> loader, BlobReference reference) {
        final byte[] content;
        try {
            content = loader.apply(reference);
        } catch (AiInvocationException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new AiInvocationException(
                    AiInvocationException.Category.AUTHORIZATION,
                    false,
                    "Governed FILE content could not be loaded",
                    failure);
        }
        if (content == null || content.length != reference.size()
                || !sha256(content).equalsIgnoreCase(reference.checksum())) {
            throw new AiInvocationException(
                    AiInvocationException.Category.AUTHORIZATION,
                    false,
                    "Governed FILE content does not match its immutable identity");
        }
        return content;
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
