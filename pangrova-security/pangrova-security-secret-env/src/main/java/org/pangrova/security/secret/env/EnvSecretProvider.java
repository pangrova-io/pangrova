package org.pangrova.security.secret.env;

import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.security.spi.secret.SecretProvider;
import org.pangrova.security.api.secret.SecretReference;

/**
 * Resolves secrets from environment variables.
 */
public final class EnvSecretProvider implements SecretProvider {

    @Override
    public String prefix() {
        return "ENV";
    }

    @Override
    public String resolve(SecretReference reference) {
        String value = System.getenv(reference.key());
        if (value == null) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_MISSING_KEY,
                    "Environment variable not found: " + reference.key());
        }
        return value;
    }
}
