package org.pangrova.server.coordinator.bootstrap;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole;
import org.pangrova.server.coordinator.testinfra.SignedOidcCoordinatorTestBase;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("integration")
class QueryPlacementBootstrapTest extends SignedOidcCoordinatorTestBase {
    @Test
    void securedQueryBootstrapNeverStartsTheUnusedDispatcherOnSuccessOrFailure() {
        Logger logger = (Logger) LoggerFactory.getLogger("org.pangrova.scheduler.resource.WeightedFairScheduler");
        var starts = new AtomicInteger();
        var capture = new AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().equals("WeightedFairScheduler started")) {
                    starts.incrementAndGet();
                }
            }
        };
        capture.setContext(logger.getLoggerContext());
        capture.start();
        logger.addAppender(capture);
        try {
            var running = new CoordinatorBootstrap(tenantConfig(CoordinatorBootstrapConfig.QueryConfig.defaults()));
            try {
                running.start();
                assertThat(running.isStarted()).isTrue();
                assertThat(starts).hasValue(0);
            } finally {
                running.shutdown();
            }

            var invalidCleanup = new CoordinatorBootstrapConfig.QueryConfig(300, 10, 0, 600, 0);
            var failed = new CoordinatorBootstrap(tenantConfig(invalidCleanup));
            try {
                assertThatThrownBy(failed::start).isInstanceOf(IllegalArgumentException.class);
                assertThat(failed.isStarted()).isFalse();
                assertThat(starts).hasValue(0);
            } finally {
                failed.shutdown();
            }
        } finally {
            logger.detachAppender(capture);
            capture.stop();
        }
    }

    private CoordinatorBootstrapConfig tenantConfig(CoordinatorBootstrapConfig.QueryConfig query) {
        return withControlAuthentication(new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "tenant-placement-lifecycle", 0, 30_000, CoordinatorDeploymentRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(false, true, false, false, false, false, true, false),
                query, CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(), null, null, null));
    }
}
