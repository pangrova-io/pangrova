package org.pangrova.security.secret;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.ConfigurationException;

/**
 * Resolves secrets from environment variables.
 */
public class EnvSecretProvider implements SecretProvider {

    @Override
    public String prefix() {
        return "ENV";
    }

    @Override
    public String resolve(String reference) {
        String value = System.getenv(reference);
        if (value == null) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_MISSING_KEY,
                    "Environment variable not found: " + reference);
        }
        return value;
    }
}
