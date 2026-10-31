package org.pangrova.coordination.inmemory;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.coordination.api.DistributedLease;

import java.net.URL;
import java.net.URLClassLoader;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProviderClasspathIsolationTest {
    @Test
    void providersConstructAndOperateWithOnlyTheirApiCommonAndJdk() throws Exception {
        URL[] locations = {
                InMemoryDistributedLease.class.getProtectionDomain().getCodeSource().getLocation(),
                DistributedLease.class.getProtectionDomain().getCodeSource().getLocation(),
                Internal.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader())) {
            for (String name : List.of("InMemoryDistributedLease", "InMemoryCoordinatorRegistry",
                    "InMemoryWorkerRegistry")) {
                var provider = loader.loadClass("org.pangrova.coordination.inmemory." + name);
                assertThat(provider.getClassLoader()).isSameAs(loader);
                assertThat(provider.getConstructor().newInstance()).isNotNull();
                provider.getMethods();
            }
            var leaseType = loader.loadClass("org.pangrova.coordination.inmemory.InMemoryDistributedLease");
            var now = Instant.parse("2026-10-31T00:00:00Z");
            var lease = leaseType.getConstructor(Clock.class).newInstance(Clock.fixed(now, ZoneOffset.UTC));
            var acquired = (Optional<?>) leaseType.getMethod("tryAcquire", String.class, String.class, Duration.class)
                    .invoke(lease, "leader", "owner", Duration.ofSeconds(30));
            assertThat(acquired).isPresent();
            var token = acquired.orElseThrow();
            assertThat(token.getClass().getMethod("epoch").invoke(token)).isEqualTo(1L);
            assertThat(token.getClass().getMethod("expiresAt").invoke(token)).isEqualTo(now.plusSeconds(30));
            assertUnavailable(loader);
        }
    }

    @Test
    void ownerDependencyClasspathContainsNoCoordinatorDatabaseOrQuorumImplementations() {
        assertUnavailable(getClass().getClassLoader());
    }

    private static void assertUnavailable(ClassLoader loader) {
        for (String name : List.of("org.pangrova.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.metadata.store.jdbc.catalog.JdbcMetadataStore", "org.postgresql.Driver",
                "org.apache.curator.framework.CuratorFramework", "org.apache.zookeeper.ZooKeeper")) {
            assertThatThrownBy(() -> loader.loadClass(name)).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
