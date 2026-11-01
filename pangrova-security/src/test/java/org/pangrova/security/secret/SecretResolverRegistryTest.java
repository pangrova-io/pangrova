package org.pangrova.security.secret;

import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.spi.security.secret.SecretProvider;
import org.pangrova.spi.security.secret.SecretReference;
import org.pangrova.spi.security.secret.SecretTemplates;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 18.3 — SecretTemplates parses ${PREFIX:reference} and
 * routes to registered providers.
 */
class SecretResolverRegistryTest {

    @Test
    void resolvesEnvSecretReference() {
        SecretResolverRegistry resolver = registryWith("TEST", reference -> "resolved-" + reference.key());

        String result = SecretTemplates.resolve(resolver, "${TEST:my-secret}", null);
        assertThat(result).isEqualTo("resolved-my-secret");
    }

    @Test
    void returnsPlainValueUnchanged() {
        SecretResolverRegistry resolver = new SecretResolverRegistry();
        assertThat(SecretTemplates.resolve(resolver, "plain-value", null)).isEqualTo("plain-value");
    }

    @Test
    void returnsNullForNullInput() {
        SecretResolverRegistry resolver = new SecretResolverRegistry();
        assertThat(SecretTemplates.resolve(resolver, null, null)).isNull();
    }

    @Test
    void returnsValueWithDollarButNoMatch() {
        SecretResolverRegistry resolver = new SecretResolverRegistry();
        assertThat(SecretTemplates.resolve(resolver, "${invalid", null)).isEqualTo("${invalid");
    }

    @Test
    void throwsOnUnknownPrefix() {
        SecretResolverRegistry resolver = new SecretResolverRegistry();
        assertThatThrownBy(() -> SecretTemplates.resolve(resolver, "${UNKNOWN:ref}", null))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("No secret provider for prefix: UNKNOWN");
    }

    @Test
    void rejectsNullProvider() {
        SecretResolverRegistry resolver = new SecretResolverRegistry();
        assertThatThrownBy(() -> resolver.register(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void laterRegistrationOverridesPrevious() {
        SecretResolverRegistry resolver = registryWith("TEST", reference -> "first");
        resolver.register(provider("TEST", reference -> "second"));

        assertThat(SecretTemplates.resolve(resolver, "${TEST:key}", null)).isEqualTo("second");
    }

    @Test
    void passesTenantScopeToProvider() {
        SecretResolverRegistry resolver = registryWith(
                "TEST", reference -> reference.tenantId() + "/" + reference.key());

        assertThat(SecretTemplates.resolve(resolver, "${TEST:key}", "tenant-a"))
                .isEqualTo("tenant-a/key");
    }

    private static SecretResolverRegistry registryWith(
            String prefix, java.util.function.Function<SecretReference, String> resolution) {
        SecretResolverRegistry resolver = new SecretResolverRegistry();
        resolver.register(provider(prefix, resolution));
        return resolver;
    }

    private static SecretProvider provider(
            String prefix, java.util.function.Function<SecretReference, String> resolution) {
        return new SecretProvider() {
            @Override
            public String prefix() {
                return prefix;
            }

            @Override
            public String resolve(SecretReference reference) {
                return resolution.apply(reference);
            }
        };
    }
}
