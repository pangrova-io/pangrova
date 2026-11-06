package org.pangrova.server.coordinator.bootstrap;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.Test;
import org.pangrova.server.coordinator.testinfra.SignedOidcCoordinatorTestBase;
import org.pangrova.scheduler.core.resource.QueueType;
import org.pangrova.scheduler.core.resource.WeightedFairScheduler;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual assembly-owned Java executor exits, not Native or distributed execution evidence. */
class QueryBootstrapFairSchedulerShutdownTest extends SignedOidcCoordinatorTestBase {
    @Test
    void factoryFailureClosesTheAlreadyAcquiredExecutorAndPreservesFailure() {
        var before = liveDispatchers();
        var acquired = new AtomicReference<Set<Thread>>();
        var failure = new IllegalStateException("provider initialization failed");
        var factory = new org.pangrova.scheduler.spi.placement.WorkerSelectionStrategyFactory() {
            @Override
            public String providerId() { return "failing-test-provider"; }

            @Override
            public int apiVersion() { return API_VERSION; }

            @Override
            public org.pangrova.scheduler.spi.placement.WorkerSelectionStrategy create() {
                acquired.set(newlyCreatedDispatchers(before));
                throw failure;
            }
        };
        assertThatThrownBy(() -> new QuerySchedulingWiring(new org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore(), factory)).isSameAs(failure);
        assertThat(acquired.get()).hasSize(1).allMatch(thread -> !thread.isAlive());
    }

    @Test
    void querySchedulingWiringOwnsAndJoinsTheActualScheduler() throws Exception {
        try (var scheduling = new QuerySchedulingWiring(new org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore())) {
            var owner = scheduling.tenantScheduler();
            assertThat(scheduling.scheduler()).isNotNull();
            var executed = new CountDownLatch(1);
            var actualThread = new AtomicReference<Thread>();
            owner.submitTask(() -> {
                actualThread.set(Thread.currentThread());
                executed.countDown();
            }, QueueType.INTERACTIVE);
            assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
            owner.close();
            assertThat(actualThread.get().isAlive()).isFalse();
        }
    }

    @Test
    void tenantDisabledCreatesNoFairSchedulerOwner() {
        try (var scheduling = new QuerySchedulingWiring(null)) {
            assertThat(scheduling.scheduler()).isNotNull();
            assertThat(scheduling.tenantScheduler()).isNull();
        }
    }

    @Test
    void securedCoordinatorShutdownJoinsItsActualTenantDispatcher() {
        var before = liveDispatchers();
        var bootstrap = new CoordinatorBootstrap(tenantConfig(CoordinatorBootstrapConfig.QueryConfig.defaults()));
        Set<Thread> owned;
        try {
            bootstrap.start();
            owned = newlyCreatedDispatchers(before);
            assertThat(owned).hasSize(1);
            bootstrap.shutdown();
            assertThat(bootstrap.isStarted()).isFalse();
            assertThat(owned).allMatch(thread -> !thread.isAlive());
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void failedStartupAfterQueryAssemblyJoinsTheCreatedTenantDispatcher() {
        var before = liveDispatchers();
        var startedOwner = new AtomicReference<Set<Thread>>();
        Logger logger = (Logger) LoggerFactory.getLogger(WeightedFairScheduler.class);
        var capture = new AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().equals("WeightedFairScheduler started")) {
                    startedOwner.set(newlyCreatedDispatchers(before));
                }
            }
        };
        capture.setContext(logger.getLoggerContext());
        capture.start();
        logger.addAppender(capture);
        var invalidCleanup = new CoordinatorBootstrapConfig.QueryConfig(300, 10, 0, 600, 0);
        var bootstrap = new CoordinatorBootstrap(tenantConfig(invalidCleanup));
        try {
            assertThatThrownBy(bootstrap::start).isInstanceOf(IllegalArgumentException.class);
            assertThat(startedOwner.get()).as("actual owner observed before the later startup failure").hasSize(1);
            assertThat(startedOwner.get()).allMatch(thread -> !thread.isAlive());
            assertThat(bootstrap.isStarted()).isFalse();
        } finally {
            logger.detachAppender(capture);
            capture.stop();
            bootstrap.shutdown();
        }
    }

    private CoordinatorBootstrapConfig tenantConfig(CoordinatorBootstrapConfig.QueryConfig query) {
        return withControlAuthentication(new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("tenant-scheduler-lifecycle", 0, 30_000, org.pangrova.server.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(false, true, false, false, false, false, true, false),
                query, CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(), null, null, null));
    }

    private static Set<Thread> liveDispatchers() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().equals("pangrova-weighted-fair-dispatcher"))
                .collect(Collectors.toUnmodifiableSet());
    }

    private static Set<Thread> newlyCreatedDispatchers(Set<Thread> before) {
        return liveDispatchers().stream().filter(thread -> !before.contains(thread))
                .collect(Collectors.toUnmodifiableSet());
    }
}
