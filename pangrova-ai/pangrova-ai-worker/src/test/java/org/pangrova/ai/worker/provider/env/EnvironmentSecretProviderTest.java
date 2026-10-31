package org.pangrova.ai.worker.provider.env;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvironmentSecretProviderTest {

    @Test
    void resolvesOnlyExplicitEnvironmentReferences() {
        var resolver = new EnvironmentSecretProvider(
                variable -> Map.of("MODEL_KEY", "secret-value").get(variable));

        assertThat(resolver.resolve("env://MODEL_KEY")).isEqualTo("secret-value");
        assertThatThrownBy(() -> resolver.resolve("literal-secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scheme://");
        assertThatThrownBy(() -> resolver.resolve("file://MODEL_KEY"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("env://");
        assertThatThrownBy(() -> resolver.resolve("env://MISSING"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing or empty");
    }
}
