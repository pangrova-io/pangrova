package org.pangrova.ai.worker.provider.env;

import org.pangrova.security.api.secret.SecretReference;
import org.pangrova.security.api.secret.SecretResolver;

import java.util.Base64;

/** Deterministic external-secret fixture used by Worker bootstrap tests. */
public final class TestSecretResolver implements SecretResolver {

    @Override
    public String resolve(SecretReference reference) {
        if ("TEST".equals(reference.prefix()) && "file-capability".equals(reference.key())) {
            byte[] key = new byte[32];
            java.util.Arrays.fill(key, (byte) 0x6b);
            return Base64.getEncoder().encodeToString(key);
        }
        throw new IllegalStateException(
                "No test secret for reference: " + reference.prefix() + "://" + reference.key());
    }
}
