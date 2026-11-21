package org.pangrova.server.coordinator.query;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.lifecycle.QueryTimeoutScheduler;

import org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.server.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Verifies QueryTimeoutScheduler is started and stopped with the
 * Coordinator lifecycle, and that it correctly times out queries.
 */
class QueryTimeoutSchedulerBootstrapTest {

    private CoordinatorBootstrap bootstrap;

    @AfterEach
    void tearDown() {
        if (bootstrap != null) {
            bootstrap.shutdown();
            bootstrap = null;
        }
    }

    @Test
    void enforcerStartsWithCoordinator() {
        bootstrap = startCoordinator();
        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getQueryManager()).isNotNull();
    }

    @Test
    void enforcerStopsOnShutdown() {
        bootstrap = startCoordinator();
        bootstrap.shutdown();
        assertThat(bootstrap.isStarted()).isFalse();
    }

    @Test
    void enforcerTimesOutExpiredQuery() throws Exception {
        QueryTimeoutScheduler enforcer = new QueryTimeoutScheduler(
                Duration.ofMillis(50), Duration.ofMillis(10));

        bootstrap = startCoordinator();
        var manager = bootstrap.getQueryManager();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        manager.registerStateMachine("q-timeout", sm);

        try {
            enforcer.start(manager);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(sm.getState()).isEqualTo(QueryState.FAILED));
        } finally {
            enforcer.stop();
        }

        assertThat(sm.getState()).isEqualTo(QueryState.FAILED);
    }

    @Test
    void enforcerSkipsNonExpiredQuery() throws Exception {
        QueryTimeoutScheduler enforcer = new QueryTimeoutScheduler(
                Duration.ofHours(1), Duration.ofMillis(10));

        bootstrap = startCoordinator();
        var manager = bootstrap.getQueryManager();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        manager.registerStateMachine("q-alive", sm);

        var checked = new CountDownLatch(1);
        try {
            enforcer.start((deadline, timeout) -> {
                manager.timeoutQueriesCreatedBy(deadline, timeout);
                checked.countDown();
            });
            assertThat(checked.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(sm.getState()).isEqualTo(QueryState.RUNNING);
        } finally {
            enforcer.stop();
            manager.cancelQuery("q-alive");
            assertThat(manager.awaitAllQueries(3000)).isTrue();
        }
    }

    private CoordinatorBootstrap startCoordinator() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(
                "coord-timeout-test", 0);
        CoordinatorBootstrap b = new CoordinatorBootstrap(withAuthority(config));
        b.start();
        return b;
    }
}
