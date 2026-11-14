package org.pangrova.security.secret.env;

import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.security.api.secret.SecretProvider;
import org.pangrova.security.api.secret.SecretReference;

import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Resolves secrets from environment variables.
 */
public final class EnvSecretProvider implements SecretProvider {

    private static final Pattern VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private final Function<String, String> environment;

    public EnvSecretProvider() {
        this(System::getenv);
    }

    EnvSecretProvider(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    @Override
    public String prefix() {
        return "ENV";
    }

    @Override
    public String resolve(SecretReference reference) {
        if (!prefix().equals(reference.prefix()) || !VARIABLE.matcher(reference.key()).matches()) {
            throw new IllegalArgumentException("Invalid environment secret reference");
        }
        String value = environment.apply(reference.key());
        if (value == null || value.isBlank()) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_MISSING_KEY,
                    "Environment variable is missing or blank");
        }
        return value;
    }
}
