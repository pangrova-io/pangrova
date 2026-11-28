package org.pangrova.coordinator.coordination;

import org.pangrova.coordination.api.enums.CoordinatorRole;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.DrainOutcome;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;
import org.pangrova.persistence.query.postgresql.state.JdbcQueryStateJournal;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies that HA critical-path metrics emit correct values.
 * Fixes finding F-COORD-HA-08: HA key path had no observability.
 */
class HaMetricsTest {

    private RecordingMetricsProvider metricsProvider;
    private HighAvailabilityCoordinationService haService;
    private HaMetrics haMetrics;

    @BeforeEach
    void setUp() {
        metricsProvider = new RecordingMetricsProvider();
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var config = new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000);
        haService = new HighAvailabilityCoordinationService(lease, config, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        haMetrics = new HaMetrics(
                metricsProvider,
                () -> haService.getRole() == CoordinatorRole.LEADER ? 1.0 : 0.0,
                () -> (double) haService.getCurrentEpoch());
    }

    @Test
    @DisplayName("role gauge returns 1.0 when leader")
    void roleGauge_leader() {
        haService.tryBecomeLeader("node-1");
        Supplier<Double> roleSupplier = metricsProvider.gaugeSupplier("pangrova.coordinator.ha.role");
        assertThat(roleSupplier).isNotNull();
        assertThat(roleSupplier.get()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("role gauge returns 0.0 when standby")
    void roleGauge_standby() {
        // Before becoming leader, role is STOPPED (gauge = 0.0)
        Supplier<Double> roleSupplier = metricsProvider.gaugeSupplier("pangrova.coordinator.ha.role");
        assertThat(roleSupplier).isNotNull();
        assertThat(roleSupplier.get()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("role gauge transitions from 1.0 to 0.0 on step-down")
    void roleGauge_transitionOnStepDown() {
        haService.tryBecomeLeader("node-1");
        Supplier<Double> roleSupplier = metricsProvider.gaugeSupplier("pangrova.coordinator.ha.role");
        assertThat(roleSupplier.get()).isEqualTo(1.0);

        haService.stepDown("node-1");
        assertThat(roleSupplier.get()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("epoch gauge reflects current epoch value")
    void epochGauge_reflectsCurrentEpoch() {
        Supplier<Double> epochSupplier = metricsProvider.gaugeSupplier("pangrova.coordinator.ha.epoch");
        assertThat(epochSupplier).isNotNull();
        double epochBefore = epochSupplier.get();

        haService.tryBecomeLeader("node-1");
        double epochAfter = epochSupplier.get();
        assertThat(epochAfter).isGreaterThan(epochBefore);
    }

    @Test
    @DisplayName("failover counter increments via recordFailover")
    void failoverCounter_incrementsOnRecord() {
        assertThat(metricsProvider.counter("pangrova.coordinator.ha.failovers")).isEqualTo(0);

        haMetrics.recordFailover();
        assertThat(metricsProvider.counter("pangrova.coordinator.ha.failovers")).isEqualTo(1);
    }

    @Test
    @DisplayName("multiple failover recordings accumulate")
    void failoverCounter_accumulates() {
        haMetrics.recordFailover();
        haMetrics.recordFailover();
        assertThat(metricsProvider.counter("pangrova.coordinator.ha.failovers")).isEqualTo(2);
    }

    @Test
    @DisplayName("renewal failure counter increments via recordRenewalFailure")
    void renewalFailureCounter_increments() {
        haMetrics.recordRenewalFailure();
        assertThat(metricsProvider.counter("pangrova.coordinator.ha.renewal.failures")).isEqualTo(1);
    }

    @Test
    @DisplayName("recovery success metric increments via recordRecoverySuccess")
    void recoverySuccess_recorded() {
        haMetrics.recordRecoverySuccess();
        assertThat(metricsProvider.counterWithAttr(
                "pangrova.coordinator.ha.recovery", "result", "success")).isEqualTo(1);
    }

    @Test
    @DisplayName("recovery failure metric increments via recordRecoveryFailure")
    void recoveryFailure_recorded() {
        haMetrics.recordRecoveryFailure();
        assertThat(metricsProvider.counterWithAttr(
                "pangrova.coordinator.ha.recovery", "result", "failure")).isEqualTo(1);
    }

    @Test
    @DisplayName("multiple recovery attempts track independently")
    void recoveryMetrics_trackIndependently() {
        haMetrics.recordRecoverySuccess();
        haMetrics.recordRecoverySuccess();
        haMetrics.recordRecoveryFailure();

        assertThat(metricsProvider.counterWithAttr(
                "pangrova.coordinator.ha.recovery", "result", "success")).isEqualTo(2);
        assertThat(metricsProvider.counterWithAttr(
                "pangrova.coordinator.ha.recovery", "result", "failure")).isEqualTo(1);
    }

    @Test
    @DisplayName("null HaMetrics does not cause NPE on HA service state transitions")
    void nullMetrics_noNpe() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var config = new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000);
        var svc = new HighAvailabilityCoordinationService(lease, config, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        // No HaMetrics set — verifying service still works without metrics
        try {
            svc.tryBecomeLeader("node-1");
            svc.stepDown("node-1");
            // No exception = pass
        } finally {
            svc.close();
        }
    }

    @Test
    @Tag("integration")
    void countsOnlyAcknowledgedDurableRetirementByTheLocalOwnerOrSuccessor() throws Exception {
        String metric = "pangrova.coordinator.queries.terminal_cleanup.durable_retirements";
        var source = (org.postgresql.ds.PGSimpleDataSource)
                SharedPostgresContainer.migratedDataSource("durable_retirement_metrics");
        var store = new JdbcExecutionAnchorStore(source);
        var config = HighAvailabilityCoordinationService.CoordinationConfig.defaults();
        var lease = new InMemoryDistributedLease();
        try (var journal = new JdbcQueryStateJournal(source,
                    new StateJournalConfig("JDBC", source.getURL(), 50, 20, 24, 100));
             var owner = new HighAvailabilityCoordinationService(lease, config, store, metricsProvider);
             var successor = new HighAvailabilityCoordinationService(lease, config, store, metricsProvider)) {
            owner.start("owner");
            successor.start("successor");

            var local = owner.acquireQueryOwnership("local", "owner", "limited", OptionalInt.of(1))
                    .orElseThrow().anchor();
            assertThat(successor.releaseLocallyDrainedQuery(local, journal.durableRetirement())).isFalse();
            assertThat(metricsProvider.counter(metric)).isZero();
            assertThat(owner.releaseLocallyDrainedQuery(local, journal.durableRetirement())).isTrue();
            assertThat(owner.isQueryRetired(local.executionId())).isTrue();
            assertThat(owner.releaseLocallyDrainedQuery(local, journal.durableRetirement())).isFalse();
            assertThat(metricsProvider.counterWithAttr(metric, "path", "local")).isEqualTo(1);

            var predecessor = owner.acquireQueryOwnership("recovered", "owner", "limited", OptionalInt.of(1))
                    .orElseThrow().anchor();
            journal.appendFencedAdmissionSync(new QueryStateEvent(
                    JournalEventContext.create(System.currentTimeMillis(), "owner"),
                    predecessor.executionId(), "SELECT 1", "{}", "QUEUED"), predecessor);
            assertThat(owner.yieldExecution(predecessor)).isTrue();
            var claims = successor.claimExpiredExecutions(1);
            assertThat(claims).hasSize(1);
            var claim = claims.get(0);
            assertThat(owner.releaseLocallyDrainedQuery(predecessor, journal.durableRetirement())).isFalse();
            assertThat(successor.releaseDrainedRecoveryClaim(claim, false)).isFalse();
            assertThat(metricsProvider.counter(metric)).isEqualTo(1);
            assertThat(journal.confirmRecoveredTaskDrains(claim)).isEqualTo(DrainOutcome.CONFIRMED);
            assertThat(metricsProvider.counter(metric)).isEqualTo(1);
            assertThat(successor.releaseDrainedRecoveryClaim(claim, false)).isTrue();
            assertThat(owner.isQueryRetired(predecessor.executionId())).isTrue();
            assertThat(successor.releaseDrainedRecoveryClaim(claim, false)).isFalse();
            assertThat(metricsProvider.counterWithAttr(metric, "path", "recovery")).isEqualTo(1);
            assertThat(metricsProvider.counter(metric)).isEqualTo(2);
        }
    }

    /**
     * In-memory MetricsProvider that records values for test assertions.
     * Supports counters (with and without attributes) and gauge supplier capture.
     */
    private static final class RecordingMetricsProvider implements MetricsProvider {
        private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> attrCounters = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Supplier<Double>> gauges = new ConcurrentHashMap<>();

        long counter(String name) {
            AtomicLong v = counters.get(name);
            return v == null ? 0L : v.get();
        }

        long counterWithAttr(String name, String attrKey, String attrValue) {
            AtomicLong v = attrCounters.get(name + ":" + attrKey + "=" + attrValue);
            return v == null ? 0L : v.get();
        }

        Supplier<Double> gaugeSupplier(String name) {
            return gauges.get(name);
        }

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            AtomicLong v = counters.computeIfAbsent(name, k -> new AtomicLong());
            return new LongCounter() {
                @Override
                public void add(long value) {
                    v.addAndGet(value);
                }

                @Override
                public void add(long value, MetricAttributes attributes) {
                    v.addAndGet(value);
                    // Also track by attribute
                    for (var entry : attributes.asMap().entrySet()) {
                        String key = name + ":" + entry.getKey() + "=" + entry.getValue();
                        attrCounters.computeIfAbsent(key, k -> new AtomicLong()).addAndGet(value);
                    }
                }
            };
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            AtomicLong v = counters.computeIfAbsent(name, k -> new AtomicLong());
            return v::addAndGet;
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                               String unit, List<Double> boundaries) {
            return value -> {};
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                 String unit, Supplier<Double> supplier) {
            gauges.put(name, supplier);
            return () -> gauges.remove(name, supplier);
        }
    }
}
