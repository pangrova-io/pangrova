package org.pangrova.coordinator.query;

import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Verifies QueryTimeoutEnforcer is started and stopped with the
 * Coordinator lifecycle, and that it correctly times out queries.
 */
class QueryTimeoutEnforcerBootstrapTest {

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
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        QueryTimeoutEnforcer enforcer = new QueryTimeoutEnforcer(
                Duration.ofMillis(50), Duration.ofMillis(10), scheduler);

        Map<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queries.put("q-timeout", sm);

        enforcer.start(() -> queries);
        Thread.sleep(200);
        enforcer.stop();

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FAILED);
    }

    @Test
    void enforcerSkipsNonExpiredQuery() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        QueryTimeoutEnforcer enforcer = new QueryTimeoutEnforcer(
                Duration.ofHours(1), Duration.ofMillis(10), scheduler);

        Map<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queries.put("q-alive", sm);

        enforcer.start(() -> queries);
        Thread.sleep(50);
        enforcer.stop();

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.RUNNING);
    }

    private CoordinatorBootstrap startCoordinator() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(
                "coord-timeout-test", 0);
        CoordinatorBootstrap b = new CoordinatorBootstrap(withAuthority(config));
        b.start();
        return b;
    }
}
