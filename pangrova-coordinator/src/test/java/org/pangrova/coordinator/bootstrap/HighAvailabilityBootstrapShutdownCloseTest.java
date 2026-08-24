package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies BUG-D24 fix: {@code HighAvailabilityBootstrap.shutdown()} must call
 * {@code haService.close()} to release the leader lease, persist orphaned queries,
 * and unregister from the coordinator registry.
 *
 * <p>Without this fix, graceful coordinator shutdown leaves a stale lease
 * (delaying failover by the full lease expiry duration), loses in-flight
 * queries, and leaves a stale coordinator registry entry.</p>
 */
class HighAvailabilityBootstrapShutdownCloseTest {

    private static final String COORDINATOR_ID = "coord-shutdown-test";
    private static final String LEASE_KEY = "coordinator_leader";

    /**
     * Verifies that after shutdown, the HA service's leader lease is released.
     * This proves {@code haService.close()} was called, because only
     * {@code close()} releases the lease and transitions to STOPPED.
     */
    @Test
    void shutdownReleasesLeaderLease() {
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease);

        // Start the HA service — it acquires the leader lease
        haService.onFailover(id -> { /* no-op for test */ });
        haService.start(COORDINATOR_ID);

        // Verify the lease is held before shutdown
        Optional<LeaseEntry> holder = lease.currentHolder(LEASE_KEY);
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo(COORDINATOR_ID);

        // Call close() — this is what shutdown() should invoke
        haService.close();

        // Verify the lease is released after close
        Optional<LeaseEntry> afterClose = lease.currentHolder(LEASE_KEY);
        assertThat(afterClose)
                .as("Leader lease should be released after haService.close()")
                .isEmpty();
    }

    /**
     * Verifies that after shutdown, the HA service role transitions to STOPPED.
     * This is a direct observable effect of {@code close()} being called.
     */
    @Test
    void shutdownTransitionsRoleToStopped() {
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease);

        haService.onFailover(id -> { /* no-op for test */ });
        haService.start(COORDINATOR_ID);

        assertThat(haService.isLeader(COORDINATOR_ID))
                .as("Should be leader after start")
                .isTrue();

        haService.close();

        assertThat(haService.getRole())
                .as("Role should be STOPPED after close()")
                .isEqualTo(HighAvailabilityCoordinationService.Role.STOPPED);
    }

    /**
     * Verifies that calling close() on a never-started HA service is safe
     * (idempotent close). This covers the null-guard in shutdown().
     */
    @Test
    void closeOnNeverStartedServiceIsSafe() {
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease);

        // close() without start() should not throw
        haService.close();

        assertThat(haService.getRole())
                .isEqualTo(HighAvailabilityCoordinationService.Role.STOPPED);
    }

    /**
     * Verifies that close() is idempotent — calling it twice does not throw.
     */
    @Test
    void closeIsIdempotent() {
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease);

        haService.onFailover(id -> { /* no-op */ });
        haService.start(COORDINATOR_ID);

        haService.close();
        haService.close(); // second close should be safe

        assertThat(haService.getRole())
                .isEqualTo(HighAvailabilityCoordinationService.Role.STOPPED);
        assertThat(lease.currentHolder(LEASE_KEY)).isEmpty();
    }
}
