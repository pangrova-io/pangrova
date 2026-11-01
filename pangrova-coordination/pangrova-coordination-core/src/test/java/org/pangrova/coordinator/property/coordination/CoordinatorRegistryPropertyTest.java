package org.pangrova.coordinator.property.coordination;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.memory.coordination.InMemoryCoordinatorRegistry;
import org.pangrova.persistence.memory.coordination.InMemoryDistributedLease;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link HighAvailabilityCoordinationService}
 * coordinator registry integration.
 *
 * <p>Each property creates its own HA service, InMemoryCoordinatorRegistry,
 * and InMemoryDistributedLease — no shared mutable state between runs.</p>
 *
 * <p>Validates: Requirements 6.1-6.6, 7.2, 7.3</p>
 */
class CoordinatorRegistryPropertyTest {

    private static final int HTTP_PORT = 8080;

    @Provide
    Arbitrary<String> coordinatorIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
                .map(s -> "coord-" + s);
    }

    @Provide
    Arbitrary<String> hosts() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
                .map(s -> "host-" + s);
    }

    /**
     * For any coordinatorId and host, after start() with a fresh lease
     * (which always wins election), getLeader() returns a snapshot with
     * matching coordinatorId and host.
     *
     * <p>Validates: Requirements 6.1, 6.2, 6.3, 6.6</p>
     */
    @Property(tries = 10)
    void afterElectionGetLeaderReturnsElectedCoordinator(
            @ForAll("coordinatorIds") String coordinatorId,
            @ForAll("hosts") String host) {

        var registry = new InMemoryCoordinatorRegistry();
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease);
        haService.setCoordinatorRegistry(registry, host, HTTP_PORT);

        try {
            haService.start(coordinatorId);

            var leader = registry.getLeader();
            assertThat(leader).isPresent();
            assertThat(leader.get().coordinatorId()).isEqualTo(coordinatorId);
            assertThat(leader.get().host()).isEqualTo(host);
            assertThat(leader.get().httpPort()).isEqualTo(HTTP_PORT);
            assertThat(leader.get().role()).isEqualTo("LEADER");
        } finally {
            haService.close();
        }
    }

    /**
     * For any coordinatorId, after start() then close(), getLeader()
     * returns empty (coordinator unregistered).
     *
     * <p>Validates: Requirements 6.5, 7.3</p>
     */
    @Property(tries = 10)
    void afterCloseGetLeaderReturnsEmpty(
            @ForAll("coordinatorIds") String coordinatorId,
            @ForAll("hosts") String host) {

        var registry = new InMemoryCoordinatorRegistry();
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease);
        haService.setCoordinatorRegistry(registry, host, HTTP_PORT);

        try {
            haService.start(coordinatorId);
            // Confirm leader is present before close
            assertThat(registry.getLeader()).isPresent();
        } finally {
            haService.close();
        }

        assertThat(registry.getLeader()).isEmpty();
        assertThat(registry.getCoordinator(coordinatorId)).isEmpty();
    }

    /**
     * For any coordinatorId, between start() and close(),
     * getCoordinator() always returns present.
     *
     * <p>Validates: Requirements 6.1, 6.2, 7.2</p>
     */
    @Property(tries = 10)
    void registryAlwaysContainsCoordinatorWhileRunning(
            @ForAll("coordinatorIds") String coordinatorId,
            @ForAll("hosts") String host) {

        var registry = new InMemoryCoordinatorRegistry();
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease);
        haService.setCoordinatorRegistry(registry, host, HTTP_PORT);

        try {
            haService.start(coordinatorId);

            // Verify coordinator is present in registry while running
            var snapshot = registry.getCoordinator(coordinatorId);
            assertThat(snapshot).isPresent();
            assertThat(snapshot.get().coordinatorId()).isEqualTo(coordinatorId);
            assertThat(snapshot.get().host()).isEqualTo(host);
            assertThat(snapshot.get().httpPort()).isEqualTo(HTTP_PORT);
        } finally {
            haService.close();
        }
    }
}
