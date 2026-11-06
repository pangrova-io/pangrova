package org.pangrova.coordinator.coordination;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for tasks 71/75: standby's first promotion to leader
 * must trigger the failover callback (notifyFailover) so that worker
 * reload and journal recovery execute.
 *
 * <p>Before the fix, onLeaderAcquired() gated notifyFailover on
 * {@code previousLeader != null}, which was always false for a
 * standby's first promotion (knownLeaderId was never set). This
 * meant the most common failover scenario silently skipped recovery.</p>
 */
class FailoverCallbackFirstPromotionTest {

    @Test
    void standbyFirstPromotion_triggersFailoverCallback() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var config = new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000);
        var svc = new HighAvailabilityCoordinationService(lease, config, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());

        AtomicReference<String> failoverLeader = new AtomicReference<>();
        AtomicInteger failoverCount = new AtomicInteger();
        svc.onFailover(leader -> {
            failoverLeader.set(leader);
            failoverCount.incrementAndGet();
        });

        try {
            // Standby first acquires leadership — previousLeader is null.
            // Before fix: notifyFailover was NOT called (previousLeader == null).
            // After fix: notifyFailover IS called for any non-self promotion.
            assertThat(svc.tryBecomeLeader("node-1")).isTrue();
            assertThat(failoverLeader.get()).isEqualTo("node-1");
            assertThat(failoverCount.get()).isEqualTo(1);
        } finally {
            svc.close();
        }
    }

    @Test
    void sameLeaderReacquire_doesNotTriggerFailover() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var config = new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000);
        var svc = new HighAvailabilityCoordinationService(lease, config, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());

        AtomicInteger failoverCount = new AtomicInteger();
        svc.onFailover(leader -> failoverCount.incrementAndGet());

        try {
            svc.tryBecomeLeader("node-1");
            assertThat(failoverCount.get()).isEqualTo(1); // first promotion

            // Same node re-acquires — should NOT trigger failover again
            svc.tryBecomeLeader("node-1");
            assertThat(failoverCount.get()).isEqualTo(1); // unchanged
        } finally {
            svc.close();
        }
    }

    @Test
    void leaderSwitchover_triggersFailoverOnNewLeader() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var config = new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000);
        var svc = new HighAvailabilityCoordinationService(lease, config, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());

        AtomicReference<String> failoverLeader = new AtomicReference<>();
        AtomicInteger failoverCount = new AtomicInteger();
        svc.onFailover(leader -> {
            failoverLeader.set(leader);
            failoverCount.incrementAndGet();
        });

        try {
            svc.tryBecomeLeader("node-1");
            assertThat(failoverCount.get()).isEqualTo(1);

            // node-1 steps down
            svc.stepDown("node-1");

            // node-2 takes over — this is a true failover
            svc.tryBecomeLeader("node-2");
            assertThat(failoverLeader.get()).isEqualTo("node-2");
            assertThat(failoverCount.get()).isEqualTo(2);
        } finally {
            svc.close();
        }
    }
}
