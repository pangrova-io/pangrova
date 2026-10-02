package org.pangrova.worker.jni.write.secret;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvironmentConnectorSecretResolverTest {
    @Test
    void resolvesOnlyTheExactEnvironmentVariable() {
        var resolver = new EnvironmentConnectorSecretResolver(Map.of("PROVIDER_KEY", "opaque-value")::get);
        assertThat(resolver.prefix()).isEqualTo("ENV");
        assertThat(resolver.resolve("authenticated-tenant", "PROVIDER_KEY")).isEqualTo("opaque-value");
    }

    @Test
    void missingVariableFailsWithoutDisclosingOtherValues() {
        var resolver = new EnvironmentConnectorSecretResolver(Map.of("OTHER_KEY", "do-not-disclose")::get);
        assertThatThrownBy(() -> resolver.resolve("authenticated-tenant", "MISSING_KEY"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Environment secret is not configured");
    }

    @Test
    void malformedVariableFailsBeforeLookingUpTheEnvironment() {
        var resolver = new EnvironmentConnectorSecretResolver(reference -> {
            throw new AssertionError("Malformed reference must never trigger environment lookup");
        });
        for (String reference : new String[] {"", "1KEY", "../KEY", "KEY=VALUE", "KEY NAME", "KEY\n"}) {
            assertThatThrownBy(() -> resolver.resolve("authenticated-tenant", reference))
                    .isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid environment secret reference");
        }
    }

    @Test
    void missingProviderIsRejectedAtConstruction() {
        assertThatThrownBy(() -> new EnvironmentConnectorSecretResolver(null))
                .isInstanceOf(NullPointerException.class).hasMessage("environment");
    }
}
