package org.pangrova.security.secret;

import org.pangrova.common.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 18.3 — SecretResolver parses ${PREFIX:reference} and routes to providers.
 */
class SecretResolverTest {

    @Test
    void resolvesEnvSecretReference() {
        SecretResolver resolver = new SecretResolver();
        resolver.register(new SecretProvider() {
            @Override public String prefix() { return "TEST"; }
            @Override public String resolve(String ref) { return "resolved-" + ref; }
        });

        String result = resolver.resolve("${TEST:my-secret}");
        assertThat(result).isEqualTo("resolved-my-secret");
    }

    @Test
    void returnsPlainValueUnchanged() {
        SecretResolver resolver = new SecretResolver();
        assertThat(resolver.resolve("plain-value")).isEqualTo("plain-value");
    }

    @Test
    void returnsNullForNullInput() {
        SecretResolver resolver = new SecretResolver();
        assertThat(resolver.resolve(null)).isNull();
    }

    @Test
    void returnsValueWithDollarButNoMatch() {
        SecretResolver resolver = new SecretResolver();
        assertThat(resolver.resolve("${invalid")).isEqualTo("${invalid");
    }

    @Test
    void throwsOnUnknownPrefix() {
        SecretResolver resolver = new SecretResolver();
        assertThatThrownBy(() -> resolver.resolve("${UNKNOWN:ref}"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("No secret provider for prefix: UNKNOWN");
    }

    @Test
    void rejectsNullProvider() {
        SecretResolver resolver = new SecretResolver();
        assertThatThrownBy(() -> resolver.register(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void laterRegistrationOverridesPrevious() {
        SecretResolver resolver = new SecretResolver();
        resolver.register(new SecretProvider() {
            @Override public String prefix() { return "TEST"; }
            @Override public String resolve(String ref) { return "first"; }
        });
        resolver.register(new SecretProvider() {
            @Override public String prefix() { return "TEST"; }
            @Override public String resolve(String ref) { return "second"; }
        });

        assertThat(resolver.resolve("${TEST:key}")).isEqualTo("second");
    }
}
