package org.pangrova.security.core.secret;

import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.security.spi.secret.SecretProvider;
import org.pangrova.security.api.secret.SecretReference;
import org.pangrova.security.core.secret.SecretTemplates;
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
    void duplicateRegistrationCannotReplaceSecretAuthority() {
        SecretResolverRegistry resolver = registryWith("TEST", reference -> "first");
        assertThatThrownBy(() -> resolver.register(provider("TEST", reference -> "second")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("already registered");

        assertThat(SecretTemplates.resolve(resolver, "${TEST:key}", null)).isEqualTo("first");
    }

    @Test
    void rejectsInvalidProviderPrefixBeforeRegistration() {
        SecretResolverRegistry resolver = new SecretResolverRegistry();
        assertThatThrownBy(() -> resolver.register(provider(null, reference -> "secret")))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> resolver.register(provider("  ", reference -> "secret")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrentRegistrationHasOneAuthorityAndDoesNotReplaceTheWinner() throws Exception {
        SecretResolverRegistry resolver = new SecretResolverRegistry();
        var ready = new java.util.concurrent.CountDownLatch(2);
        var start = new java.util.concurrent.CountDownLatch(1);
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var registrations = java.util.stream.IntStream.range(0, 2).mapToObj(index -> executor.submit(() -> {
                ready.countDown();
                if (!start.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    throw new AssertionError("Registration race did not start");
                }
                try {
                    resolver.register(provider("TEST", reference -> "provider-" + index));
                    return "provider-" + index;
                } catch (IllegalArgumentException duplicate) {
                    return null;
                }
            })).toList();
            assertThat(ready.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            start.countDown();
            var winners = new java.util.ArrayList<String>();
            for (var registration : registrations) {
                String winner = registration.get(5, java.util.concurrent.TimeUnit.SECONDS);
                if (winner != null) {
                    winners.add(winner);
                }
            }
            assertThat(winners).hasSize(1);
            assertThat(SecretTemplates.resolve(resolver, "${TEST:key}", null)).isEqualTo(winners.get(0));
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
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
