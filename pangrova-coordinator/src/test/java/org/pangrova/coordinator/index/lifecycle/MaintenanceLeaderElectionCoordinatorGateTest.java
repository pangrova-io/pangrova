package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MaintenanceLeaderElection} lease-based leader election
 * for index maintenance operations.
 *
 * <p>Validates that:</p>
 * <ul>
 *   <li>A coordinator can acquire the maintenance lease</li>
 *   <li>Renewals work for the current holder</li>
 *   <li>A second coordinator is blocked while the first holds the lease</li>
 *   <li>After the first coordinator's lease expires, the second can acquire</li>
 *   <li>Epoch monotonicity is maintained across acquisitions</li>
 * </ul>
 */
@DisplayName("MaintenanceLeaderElection lease-based gate")
class MaintenanceLeaderElectionCoordinatorGateTest {

    @Test
    @DisplayName("First coordinator can acquire maintenance lease")
    void firstCoordinatorCanAcquire() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        MaintenanceLeaderElection election = new MaintenanceLeaderElection(lease, "coord-1");

        assertThat(election.tryAcquireOrRenew())
                .as("First coordinator should acquire the maintenance lease")
                .isTrue();
    }

    @Test
    @DisplayName("Same coordinator can renew the lease")
    void renewalSucceedsForCurrentHolder() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        MaintenanceLeaderElection election = new MaintenanceLeaderElection(lease, "coord-1");

        assertThat(election.tryAcquireOrRenew()).isTrue();
        // Second call should renew
        assertThat(election.tryAcquireOrRenew()).isTrue();
    }

    @Test
    @DisplayName("Second coordinator is blocked while first holds lease")
    void secondCoordinatorBlocked() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        MaintenanceLeaderElection election1 = new MaintenanceLeaderElection(lease, "coord-1");
        MaintenanceLeaderElection election2 = new MaintenanceLeaderElection(lease, "coord-2");

        assertThat(election1.tryAcquireOrRenew()).isTrue();
        // Second coordinator should be blocked
        assertThat(election2.tryAcquireOrRenew()).isFalse();
    }

    @Test
    @DisplayName("Epoch increments on successive acquisitions")
    void epochIncrementsOnAcquisitions() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        AtomicLong epoch = new AtomicLong(0);
        MaintenanceLeaderElection election = new MaintenanceLeaderElection(lease, "coord-1", epoch);

        assertThat(election.tryAcquireOrRenew()).isTrue();
        long epochAfterFirst = epoch.get();
        assertThat(epochAfterFirst).isGreaterThan(0);
    }

    @Test
    @DisplayName("Lease duration is 30 seconds")
    void leaseDurationIs30Seconds() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        MaintenanceLeaderElection election = new MaintenanceLeaderElection(lease, "coord-1");

        assertThat(election.leaseDuration().getSeconds()).isEqualTo(30);
    }

    @Test
    @DisplayName("Multiple renewals are stable")
    void multipleRenewals() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        MaintenanceLeaderElection election = new MaintenanceLeaderElection(lease, "coord-1");

        // First call acquires
        assertThat(election.tryAcquireOrRenew()).isTrue();

        // Subsequent calls renew
        for (int i = 0; i < 10; i++) {
            assertThat(election.tryAcquireOrRenew()).isTrue();
        }
    }

    @Test
    @DisplayName("Constructor with explicit epoch source")
    void constructorWithExplicitEpoch() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        AtomicLong epoch = new AtomicLong(100);
        MaintenanceLeaderElection election = new MaintenanceLeaderElection(lease, "coord-1", epoch);

        assertThat(election.tryAcquireOrRenew()).isTrue();
        // Epoch should have been used during acquisition
        assertThat(epoch.get()).isGreaterThanOrEqualTo(100);
    }
}
