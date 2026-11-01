package org.pangrova.worker.nativebridge.jni.write.secret.env;

import org.pangrova.spi.security.secret.SecretProvider;
import org.pangrova.spi.security.secret.SecretReference;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.spi.connector.ConnectorSecretResolver;

import java.util.function.Function;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Worker-local ENV secret provider and connector consumer adapter; values never
 * enter TaskRequest.
 */
@Internal
public final class EnvironmentConnectorSecretResolver
        implements ConnectorSecretResolver, SecretProvider {

    private static final String PREFIX = "ENV";
    private static final Pattern VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private final Function<String, String> environment;

    public EnvironmentConnectorSecretResolver() {
        this(System::getenv);
    }

    EnvironmentConnectorSecretResolver(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    @Override
    public String prefix() {
        return PREFIX;
    }

    @Override
    public String resolve(String tenantId, String reference) {
        requireReference(reference);
        return resolve(new SecretReference(PREFIX, reference, tenantId));
    }

    @Override
    public String resolve(SecretReference reference) {
        if (!PREFIX.equals(reference.prefix())) {
            throw new IllegalArgumentException("Unsupported environment secret prefix");
        }
        requireReference(reference.key());
        String value = environment.apply(reference.key());
        if (value == null) {
            throw new IllegalStateException("Environment secret is not configured");
        }
        return value;
    }

    private static void requireReference(String reference) {
        if (reference == null || !VARIABLE.matcher(reference).matches()) {
            throw new IllegalArgumentException("Invalid environment secret reference");
        }
    }
}
