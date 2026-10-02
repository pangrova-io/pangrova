package org.pangrova.worker.jni.write.secret;

import org.pangrova.spi.annotation.Internal;

import org.pangrova.spi.connector.ConnectorSecretResolver;

import java.util.function.Function;
import java.util.Objects;
import java.util.regex.Pattern;

/** Worker-local ENV secret provider; values never enter TaskRequest. */
@Internal
public final class EnvironmentConnectorSecretResolver implements ConnectorSecretResolver {

    private static final Pattern VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private final Function<String, String> environment;

    public EnvironmentConnectorSecretResolver() {
        this(System::getenv);
    }

    EnvironmentConnectorSecretResolver(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    @Override
    public String prefix() { return "ENV"; }

    @Override
    public String resolve(String tenantId, String reference) {
        if (!VARIABLE.matcher(reference).matches()) {
            throw new IllegalArgumentException("Invalid environment secret reference");
        }
        String value = environment.apply(reference);
        if (value == null) {
            throw new IllegalStateException("Environment secret is not configured");
        }
        return value;
    }
}
