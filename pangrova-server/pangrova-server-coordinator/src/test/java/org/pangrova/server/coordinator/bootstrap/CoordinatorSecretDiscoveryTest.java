package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.security.api.secret.SecretProvider;
import org.pangrova.security.api.secret.SecretReference;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatorSecretDiscoveryTest {
    @Test
    void discoversPackagedProvidersAndDispatchesEachReferenceToItsOwner() {
        var loader = CoordinatorBootstrap.class.getClassLoader();
        var prefixes = ServiceLoader.load(SecretProvider.class, loader).stream()
                .map(factory -> factory.get().prefix()).toList();
        assertThat(prefixes).contains("ENV", "FILE").doesNotHaveDuplicates();
        var resolver = SecretResolverRegistry.discover(loader);
        assertThat(resolver.resolve(SecretReference.of("ENV", "PATH"))).isEqualTo(System.getenv("PATH"));
        assertThatThrownBy(() -> resolver.resolve(SecretReference.of("FILE", "../escape")))
                .isInstanceOf(ConfigurationException.class).hasMessageContaining("outside allowed directory");
        assertThatThrownBy(() -> resolver.resolve(SecretReference.of("UNINSTALLED", "reference")))
                .isInstanceOf(ConfigurationException.class).hasMessageContaining("No secret provider for prefix");
    }
}
