package org.pangrova.persistence.coordination.memory;

import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.persistence.coordination.memory.membership.InMemoryCoordinatorRegistry;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;

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
                LeaseEntry.class.getProtectionDomain().getCodeSource().getLocation(),
                DistributedLease.class.getProtectionDomain().getCodeSource().getLocation(),
                Internal.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader())) {
            for (Class<?> type : List.of(InMemoryDistributedLease.class, InMemoryCoordinatorRegistry.class,
                    InMemoryWorkerRegistry.class)) {
                var provider = loader.loadClass(type.getName());
                assertThat(provider.getClassLoader()).isSameAs(loader);
                assertThat(provider.getConstructor().newInstance()).isNotNull();
                provider.getMethods();
            }
            var leaseType = loader.loadClass("org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease");
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
        for (String name : List.of("org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository", "org.postgresql.Driver",
                "org.pangrova.query.core.audit.QueryAuditService",
                "org.pangrova.persistence.query.postgresql.transaction.JdbcWriteCommitJournal",
                "org.pangrova.security.core.authorization.deferred.DeferredAuthorization",
                "org.pangrova.protocol.v2.TaskRequest",
                "org.apache.curator.framework.CuratorFramework", "org.apache.zookeeper.ZooKeeper")) {
            assertThatThrownBy(() -> loader.loadClass(name)).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
