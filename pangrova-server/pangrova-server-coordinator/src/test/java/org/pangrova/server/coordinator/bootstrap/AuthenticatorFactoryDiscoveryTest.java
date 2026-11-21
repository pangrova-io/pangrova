package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.security.api.authentication.AuthenticationRequest;
import org.pangrova.security.api.authentication.Authenticator;
import org.pangrova.security.api.authentication.AuthenticatorFactory;
import org.pangrova.security.auth.BypassAuthenticator;
import org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrapConfig.SecurityConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthenticatorFactoryDiscoveryTest {
    @Test
    void installedFactoriesCreateDistinctBackendsAndOnlyExplicitNoopAdmitsAnonymous() {
        for (var entry : Map.of("noop", "bypass", "ldap", "ldap", "oauth2", "oauth2").entrySet()) {
            var factory = CoordinatorAuthenticatorFactories.select(entry.getKey());
            try (var first = factory.create(); var second = factory.create()) {
                assertThat(first).isNotSameAs(second);
                assertThat(first.getName()).isEqualToIgnoringCase(entry.getValue());
                assertThat(first.bypassesAuthentication()).isEqualTo(entry.getKey().equals("noop"));
            }
        }
        try (var manager = SecurityBootstrap.startAuthentication(SecurityConfig.noop());
                var request = new AuthenticationRequest(null, null, null, null, Map.of(), "127.0.0.1")) {
            assertThat(manager.authenticate(request).userIdentity().principal()).isEqualTo("anonymous");
        }
    }

    @Test
    void rejectsMissingDuplicateAndIncompatibleFactoriesBeforeStartup() {
        var tracker = new TrackingAuthenticator(false);
        var factory = new Factory("INTERNAL", AuthenticatorFactory.API_VERSION, tracker);
        assertThatThrownBy(() -> CoordinatorAuthenticatorFactories.select("unknown", List.of(factory)))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("Unknown security.auth.type");
        assertThatThrownBy(() -> CoordinatorAuthenticatorFactories.select("INTERNAL", List.of(factory, factory)))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("Duplicate");
        var incompatible = new Factory("OTHER", AuthenticatorFactory.API_VERSION + 1, tracker);
        assertThatThrownBy(() -> CoordinatorAuthenticatorFactories.select("INTERNAL", List.of(factory, incompatible)))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("Invalid authenticator factory");
        assertThat(tracker.initializations).isZero();
    }

    @Test
    void rejectsBypassCapabilityMismatchBeforeInitAndClosesRejectedInstance() {
        for (String id : List.of("NOOP", "INTERNAL")) {
            var instance = new TrackingAuthenticator(!id.equals("NOOP"));
            var factory = new Factory(id, AuthenticatorFactory.API_VERSION, instance);
            var config = new SecurityConfig(factory, "internal", Map.of());
            assertThatThrownBy(() -> SecurityBootstrap.startAuthentication(config))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("bypass capability");
            assertThat(instance.initializations).isZero();
            assertThat(instance.closes).isEqualTo(1);
        }
    }

    @Test
    void providerCannotExposeAPropertyThatEnablesBypass() {
        var instance = new TrackingAuthenticator(false);
        var factory = new Factory("INTERNAL", AuthenticatorFactory.API_VERSION, instance);
        assertThatThrownBy(() -> new SecurityConfig(factory, "internal", Map.of("bypass.enabled", "true")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("NOOP");
        assertThat(instance.initializations).isZero();
    }

    @Test
    void failedInitializationClosesExactlyOnceAndPreservesBothFailures() {
        var instance = new TrackingAuthenticator(false);
        instance.initFailure = new IllegalStateException("backend connection failed");
        instance.closeFailure = new IllegalArgumentException("cleanup failed");
        var factory = new Factory("INTERNAL", AuthenticatorFactory.API_VERSION, instance);
        assertThatThrownBy(() -> SecurityBootstrap.startAuthentication(new SecurityConfig(factory, null, Map.of())))
                .isSameAs(instance.initFailure);
        assertThat(instance.initFailure.getSuppressed()).containsExactly(instance.closeFailure);
        assertThat(instance.initializations).isOne();
        assertThat(instance.closes).isOne();
    }

    private record Factory(String providerId, int apiVersion, Authenticator authenticator)
            implements AuthenticatorFactory {
        @Override
        public Map<String, String> configurationKeys() {
            return Map.of();
        }

        @Override
        public void validate(Map<String, String> properties) {}

        @Override
        public Authenticator create() {
            return authenticator;
        }
    }

    private static final class TrackingAuthenticator extends BypassAuthenticator {
        private final boolean bypass;
        private int initializations;
        private int closes;
        private RuntimeException initFailure;
        private RuntimeException closeFailure;

        private TrackingAuthenticator(boolean bypass) {
            this.bypass = bypass;
        }

        @Override
        public boolean bypassesAuthentication() {
            return bypass;
        }

        @Override
        public void init(Map<String, String> properties) {
            initializations++;
            if (initFailure != null) {
                throw initFailure;
            }
        }

        @Override
        public void close() {
            closes++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
