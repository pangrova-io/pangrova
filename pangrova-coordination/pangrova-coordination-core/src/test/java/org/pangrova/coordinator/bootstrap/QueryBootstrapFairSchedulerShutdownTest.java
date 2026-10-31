package org.pangrova.coordinator.bootstrap;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.bootstrap.module.DefaultModuleContext;
import org.pangrova.coordinator.bootstrap.module.QueryExecutionModule;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.testinfra.SignedOidcCoordinatorTestBase;
import org.pangrova.scheduler.resource.QueueType;
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
    void queryExecutionModulePublishesTheActualOwnedScheduler() throws Exception {
        var context = new DefaultModuleContext();
        new QueryExecutionModule(true).configure(context);
        try (var owner = context.require(WeightedFairScheduler.class)) {
            assertThat(context.require(TaskScheduler.class)).isNotNull();
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
        var context = new DefaultModuleContext();
        new QueryExecutionModule(false).configure(context);
        assertThat(context.require(TaskScheduler.class)).isNotNull();
        assertThat(context.optional(WeightedFairScheduler.class)).isNull();
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
                new CoordinatorBootstrapConfig.CoreConfig("tenant-scheduler-lifecycle", 0, 30_000, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
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
