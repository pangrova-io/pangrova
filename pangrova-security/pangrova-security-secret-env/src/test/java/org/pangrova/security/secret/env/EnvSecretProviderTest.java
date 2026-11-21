package org.pangrova.security.secret.env;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.security.api.secret.SecretReference;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvSecretProviderTest {
    @Test
    void resolvesOnlyTheExactEnvironmentVariable() {
        var resolver = new EnvSecretProvider(Map.of("PROVIDER_KEY", "opaque-value")::get);
        assertThat(resolver.prefix()).isEqualTo("ENV");
        assertThat(resolver.resolve(new SecretReference("ENV", "PROVIDER_KEY", "authenticated-tenant"))).isEqualTo("opaque-value");
    }

    @Test
    void missingVariableFailsWithoutDisclosingOtherValues() {
        var resolver = new EnvSecretProvider(Map.of("OTHER_KEY", "do-not-disclose")::get);
        assertThatThrownBy(() -> resolver.resolve(new SecretReference("ENV", "MISSING_KEY", "authenticated-tenant")))
                .isInstanceOf(ConfigurationException.class).hasMessageNotContaining("do-not-disclose");
    }

    @Test
    void malformedVariableFailsBeforeLookingUpTheEnvironment() {
        var resolver = new EnvSecretProvider(reference -> {
            throw new AssertionError("Malformed reference must never trigger environment lookup");
        });
        for (String reference : new String[] {"", "1KEY", "../KEY", "KEY=VALUE", "KEY NAME", "KEY\n"}) {
            assertThatThrownBy(() -> resolver.resolve(new SecretReference("ENV", reference, "authenticated-tenant")))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void aiReferencesRequireAnExplicitSchemeAndNonblankSecret() {
        var provider = new EnvSecretProvider(Map.of("MODEL_KEY", "value", "BLANK", " ")::get);
        assertThat(provider.resolve("env://MODEL_KEY")).isEqualTo("value");
        assertThatThrownBy(() -> provider.resolve("literal")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.resolve("file://MODEL_KEY")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.resolve("env://BLANK")).isInstanceOf(ConfigurationException.class);
    }

    @Test
    void missingProviderIsRejectedAtConstruction() {
        assertThatThrownBy(() -> new EnvSecretProvider(null))
                .isInstanceOf(NullPointerException.class).hasMessage("environment");
    }
}
