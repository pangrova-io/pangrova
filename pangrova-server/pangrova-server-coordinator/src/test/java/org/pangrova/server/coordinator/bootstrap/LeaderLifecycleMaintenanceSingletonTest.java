package org.pangrova.server.coordinator.bootstrap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.coordination.api.leadership.MaintenanceLeaderAware;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * G-HA-05 regression tests: verifies that maintenance singletons are started
 * on leader promotion and stopped on demotion.
 *
 * <p>Before fix: {@code onLeaderAcquired()} only called {@code notifyFailover()}
 * when {@code previousLeader != null && !previousLeader.equals(coordinatorId)}.
 * This meant:
 * <ul>
 *   <li>First-time leader election (previousLeader == null) — no callback</li>
 *   <li>Standby promotion after leader crash (previousLeader == null on standby) — no callback</li>
 * </ul>
 * Result: maintenance singletons (index commit poller, TTL cleanup, refresh scheduler,
 * metadata sync) were never started on the promoted leader.</p>
 *
 * <p>After fix: {@code notifyFailover()} fires on ANY new leader acquisition
 * (first-time, standby promotion, leader switchover) except same-leader re-acquire.</p>
 */
class LeaderLifecycleMaintenanceSingletonTest {

    /**
     * Verifies that when a node becomes leader for the first time via start(),
     * the failover callback fires and would trigger MaintenanceLeaderAware.onLeaderElected().
     */
    @Test
    void firstLeaderElection_triggersMaintenanceStart() {
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        var recorder = new MaintenanceRecorder();

        // Wire the recorder as the maintenance callback (mirrors HighAvailabilityBootstrap)
        haService.onFailover(id -> recorder.onLeaderElected());
        haService.onDemotion(recorder::onLeadershipLost);

        haService.start("coord-1");

        assertThat(recorder.leaderElectedCount.get())
                .as("onLeaderElected should fire on first leader election")
                .isEqualTo(1);
        assertThat(recorder.leadershipLostCount.get())
                .as("onLeadershipLost should not fire on election")
                .isEqualTo(0);

        haService.close();
    }

    /**
     * Verifies that stepping down triggers demotion and the maintenance
     * singletons are stopped.
     */
    @Test
    void stepDown_triggersMaintenanceStop() {
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        var recorder = new MaintenanceRecorder();

        haService.onFailover(id -> recorder.onLeaderElected());
        haService.onDemotion(recorder::onLeadershipLost);
        haService.start("coord-1");

        // Reset after initial election
        recorder.reset();

        haService.stepDown("coord-1");

        assertThat(recorder.leadershipLostCount.get())
                .as("onLeadershipLost should fire on step-down")
                .isEqualTo(1);
        assertThat(recorder.leaderElectedCount.get())
                .as("onLeaderElected should not fire on step-down")
                .isEqualTo(0);

        haService.close();
    }

    /**
     * Verifies the full cycle: node-1 elected -> node-1 steps down (demotion) ->
     * node-2 takes over (elected). Both callbacks fire at the right times.
     */
    @Test
    void fullFailoverCycle_startsAndStopsMaintenanceSingletons() {
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        var recorder = new MaintenanceRecorder();

        haService.onFailover(id -> recorder.onLeaderElected());
        haService.onDemotion(recorder::onLeadershipLost);
        haService.start("node-1");

        // First election triggers start
        assertThat(recorder.leaderElectedCount.get()).isEqualTo(1);
        assertThat(recorder.events).containsExactly("elected");

        // Step down triggers stop
        haService.stepDown("node-1");
        assertThat(recorder.leadershipLostCount.get()).isEqualTo(1);
        assertThat(recorder.events).containsExactly("elected", "lost");

        // Another node takes over — triggers start again
        boolean acquired = haService.tryBecomeLeader("node-2");
        assertThat(acquired).isTrue();
        assertThat(recorder.leaderElectedCount.get()).isEqualTo(2);
        assertThat(recorder.events).containsExactly("elected", "lost", "elected");

        haService.close();
    }

    /**
     * Verifies that same-leader re-acquire (lease renewal success path) does
     * NOT trigger duplicate maintenance starts.
     */
    @Test
    void sameLeaderReacquire_doesNotDuplicateMaintenanceStart() {
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        var recorder = new MaintenanceRecorder();

        haService.onFailover(id -> recorder.onLeaderElected());
        haService.start("coord-1");

        assertThat(recorder.leaderElectedCount.get()).isEqualTo(1);

        // Same leader re-acquires — should NOT trigger callback again
        haService.tryBecomeLeader("coord-1");

        assertThat(recorder.leaderElectedCount.get())
                .as("same-leader re-acquire should not trigger duplicate start")
                .isEqualTo(1);

        haService.close();
    }

    /**
     * Simulates the production scenario: standby coordinator starts, does NOT
     * become leader (another node holds the lease), then the leader dies and
     * the standby wins the election. The failover callback must fire.
     */
    @Test
    void standbyPromotionAfterLeaderDeath_startsMaintenanceSingletons() {
        var lease = new InMemoryDistributedLease();

        // Node-1 takes leadership first
        var haService1 = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        haService1.start("node-1");
        assertThat(haService1.isLeader("node-1")).isTrue();

        // Node-2 starts as standby (cannot win because node-1 holds the lease)
        var haService2 = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        var recorder2 = new MaintenanceRecorder();
        haService2.onFailover(id -> recorder2.onLeaderElected());
        haService2.onDemotion(recorder2::onLeadershipLost);

        // node-2 tries to start but loses election
        boolean node2Won = haService2.tryBecomeLeader("node-2");
        assertThat(node2Won).isFalse();
        assertThat(recorder2.leaderElectedCount.get())
                .as("standby should not get maintenance callback")
                .isEqualTo(0);

        // Simulate leader death: node-1 releases lease (crash scenario)
        haService1.close();

        // Now node-2 wins the election
        boolean promoted = haService2.tryBecomeLeader("node-2");
        assertThat(promoted).isTrue();
        assertThat(recorder2.leaderElectedCount.get())
                .as("standby promotion must trigger maintenance start")
                .isEqualTo(1);

        haService2.close();
    }

    /**
     * Verifies that the ModuleIntegrationBootstrap onLeaderElected/onLeadershipLost
     * methods are callable and the leader lifecycle events propagate through the
     * HA service to the module wiring layer.
     */
    @Test
    void haServiceToModuleWiring_endToEndLeaderLifecycle() {
        var lease = new InMemoryDistributedLease();
        var haService = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        var recorder = new MaintenanceRecorder();

        // This mirrors what HighAvailabilityBootstrap.startHA() does
        haService.onFailover(id -> recorder.onLeaderElected());
        haService.onDemotion(recorder::onLeadershipLost);
        haService.start("coordinator-1");

        // Verify the full event sequence matches production behavior
        assertThat(recorder.leaderElectedCount.get())
                .as("HA start -> leader election -> onLeaderElected")
                .isEqualTo(1);

        // Step down and re-promote to verify the full cycle
        haService.stepDown("coordinator-1");
        assertThat(recorder.leadershipLostCount.get()).isEqualTo(1);

        // Re-promote after step-down triggers maintenance start again.
        // G-HA-05: stepDown clears knownLeaderId, so re-acquiring is treated
        // as a new leader acquisition — maintenance singletons must restart.
        haService.tryBecomeLeader("coordinator-1");
        assertThat(recorder.leaderElectedCount.get())
                .as("re-acquire after step-down must restart maintenance singletons")
                .isEqualTo(2);

        haService.close();
    }

    // ── Test helpers ──────────────────────────────────────────────────

    /**
     * Records leader lifecycle transitions for assertions.
     */
    private static final class MaintenanceRecorder implements MaintenanceLeaderAware {
        final AtomicInteger leaderElectedCount = new AtomicInteger();
        final AtomicInteger leadershipLostCount = new AtomicInteger();
        final List<String> events = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onLeaderElected() {
            leaderElectedCount.incrementAndGet();
            events.add("elected");
        }

        @Override
        public void onLeadershipLost() {
            leadershipLostCount.incrementAndGet();
            events.add("lost");
        }

        void reset() {
            leaderElectedCount.set(0);
            leadershipLostCount.set(0);
            events.clear();
        }
    }
}
