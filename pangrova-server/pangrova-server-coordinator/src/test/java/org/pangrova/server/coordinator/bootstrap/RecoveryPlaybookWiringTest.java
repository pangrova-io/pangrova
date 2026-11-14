package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.operations.recovery.ErrorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.server.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Verifies that {@link org.pangrova.operations.recovery.RecoveryPlaybook} is wired into the Coordinator
 * bootstrap and records unhandled incidents as escalations.
 *
 * <p>Validates: Requirement R6.1</p>
 */
class RecoveryPlaybookWiringTest {

    private CoordinatorBootstrap bootstrap;

    @AfterEach
    void tearDown() {
        if (bootstrap != null) {
            bootstrap.shutdown();
            bootstrap = null;
        }
    }

    @Test
    void operationsEnabled_recoveryPlaybookInstantiated() {
        try (var wiring = new ModuleIntegrationBootstrap()) {
            wiring.startOperations(operationsEnabled());
            assertThat(wiring.getRecoveryPlaybook()).isNotNull();
        }
    }

    @Test
    void operationsDisabled_recoveryPlaybookIsNull() {
        try (var wiring = new ModuleIntegrationBootstrap()) {
            wiring.startOperations(CoordinatorBootstrapConfig.withDefaults("coord-test", 0));
            assertThat(wiring.getRecoveryPlaybook()).isNull();
        }
    }

    @Test
    void unhandledOperationalFailuresEscalateWithoutClaimingRecovery() {
        try (var wiring = new ModuleIntegrationBootstrap()) {
            wiring.startOperations(operationsEnabled());
            var playbook = wiring.getRecoveryPlaybook();
            for (String code : java.util.List.of("WORKER_DEAD", "TASK_TIMEOUT", "OOM_KILLED", "UNKNOWN_ERROR")) {
                assertThat(playbook.hasAction(code)).isFalse();
                playbook.handle(new ErrorContext(code, "requires operator intervention", Instant.now()));
            }
            assertThat(playbook.executedCount()).isZero();
            assertThat(playbook.escalatedCount()).isEqualTo(4);
        }
    }

    @Test
    void fullBootstrap_recoveryPlaybookAccessibleViaCoordinatorBootstrap() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("coord-recovery", 0,
                        CoordinatorBootstrapConfig.CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.server.coordinator.enums.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, false, false, false, false, true),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);
        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThat(bootstrap.getRecoveryPlaybook()).isNotNull();
    }

    @Test
    void fullBootstrap_operationsDisabled_recoveryPlaybookIsNull() {
        bootstrap = new CoordinatorBootstrap(withAuthority(
                CoordinatorBootstrapConfig.withDefaults("coord-no-ops", 0)));
        bootstrap.start();

        assertThat(bootstrap.getRecoveryPlaybook()).isNull();
    }

    private static CoordinatorBootstrapConfig operationsEnabled() {
        return new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("coord-ops", 0,
                        CoordinatorBootstrapConfig.CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.server.coordinator.enums.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, false, false, false, false, true),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);
    }
}
