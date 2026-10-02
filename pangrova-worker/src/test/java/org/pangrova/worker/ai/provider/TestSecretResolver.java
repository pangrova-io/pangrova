package org.pangrova.worker.ai.provider;

import org.pangrova.spi.ai.embedding.SecretResolver;

import java.util.Base64;

/** Deterministic external-secret fixture used by Worker bootstrap tests. */
public final class TestSecretResolver implements SecretResolver {

    @Override
    public String resolve(String secretReference) {
        if ("test://file-capability".equals(secretReference)) {
            byte[] key = new byte[32];
            java.util.Arrays.fill(key, (byte) 0x6b);
            return Base64.getEncoder().encodeToString(key);
        }
        throw new IllegalStateException("No test secret for reference: " + secretReference);
    }
}
