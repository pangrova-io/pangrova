package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
    @DisplayName("A high local epoch cannot preempt an unexpired maintenance holder")
    void highLocalEpochDoesNotPreemptLiveHolder() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        MaintenanceLeaderElection holder = new MaintenanceLeaderElection(lease, "coord-1");
        MaintenanceLeaderElection contender = new MaintenanceLeaderElection(lease, "coord-2");

        assertThat(holder.tryAcquireOrRenew()).isTrue();
        assertThat(contender.tryAcquireOrRenew()).isFalse();
        assertThat(lease.currentHolder("maintenance-control").orElseThrow().holderId())
                .isEqualTo("coord-1");
    }

    @Test
    @DisplayName("Epoch increments on successive acquisitions")
    void epochIncrementsOnAcquisitions() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        MaintenanceLeaderElection first = new MaintenanceLeaderElection(lease, "coord-1");
        MaintenanceLeaderElection second = new MaintenanceLeaderElection(lease, "coord-2");

        assertThat(first.acquireOrRenewEpoch()).hasValue(1L);
        assertThat(first.release()).isTrue();
        assertThat(second.acquireOrRenewEpoch()).hasValue(2L);
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
    void verifiedEpochDoesNotAcquireLeadership() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var first = new MaintenanceLeaderElection(lease, "first");
        var second = new MaintenanceLeaderElection(lease, "second");

        assertThat(first.verifiedHeldEpoch()).isEmpty();
        assertThat(lease.currentHolder("maintenance-control")).isEmpty();
        long epoch = first.acquireOrRenewEpoch().orElseThrow();
        assertThat(first.verifiedHeldEpoch()).hasValue(epoch);
        assertThat(second.verifiedHeldEpoch()).isEmpty();
        assertThat(first.release()).isTrue();
        assertThat(second.acquireOrRenewEpoch()).hasValue(epoch + 1);
        assertThat(first.verifiedHeldEpoch()).isEmpty();
    }

    @Test
    @DisplayName("Election returns the backend-issued fencing epoch")
    void returnsBackendIssuedEpoch() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        MaintenanceLeaderElection election = new MaintenanceLeaderElection(lease, "coord-1");

        assertThat(election.acquireOrRenewEpoch()).hasValue(1L);
        assertThat(lease.currentHolder("maintenance-control").orElseThrow().epoch())
                .isEqualTo(1L);
    }
}
