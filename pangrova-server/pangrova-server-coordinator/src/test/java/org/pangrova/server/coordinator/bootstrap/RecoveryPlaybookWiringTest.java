package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.operations.recovery.ErrorContext;
import org.pangrova.operations.recovery.RecoveryPlaybookFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.pangrova.server.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Verifies that {@link RecoveryPlaybookFramework} is wired into the Coordinator
 * bootstrap with WORKER_DEAD, TASK_TIMEOUT, and OOM_KILLED recovery actions.
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
    void workerDeadActionRegistered() {
        try (var wiring = new ModuleIntegrationBootstrap()) {
            wiring.startOperations(operationsEnabled());
            RecoveryPlaybookFramework playbook = wiring.getRecoveryPlaybook();
            ErrorContext ctx = new ErrorContext("WORKER_DEAD", "worker-1 lost", Instant.now());
            assertThatCode(() -> playbook.handle("WORKER_DEAD", ctx)).doesNotThrowAnyException();
        }
    }

    @Test
    void taskTimeoutActionRegistered() {
        try (var wiring = new ModuleIntegrationBootstrap()) {
            wiring.startOperations(operationsEnabled());
            RecoveryPlaybookFramework playbook = wiring.getRecoveryPlaybook();
            ErrorContext ctx = new ErrorContext("TASK_TIMEOUT", "task-1 exceeded deadline", Instant.now());
            assertThatCode(() -> playbook.handle("TASK_TIMEOUT", ctx)).doesNotThrowAnyException();
        }
    }

    @Test
    void oomKilledActionRegistered() {
        try (var wiring = new ModuleIntegrationBootstrap()) {
            wiring.startOperations(operationsEnabled());
            RecoveryPlaybookFramework playbook = wiring.getRecoveryPlaybook();
            ErrorContext ctx = new ErrorContext("OOM_KILLED", "task-2 OOM", Instant.now());
            assertThatCode(() -> playbook.handle("OOM_KILLED", ctx)).doesNotThrowAnyException();
        }
    }

    @Test
    void unregisteredErrorCode_escalatesWithoutException() {
        try (var wiring = new ModuleIntegrationBootstrap()) {
            wiring.startOperations(operationsEnabled());
            RecoveryPlaybookFramework playbook = wiring.getRecoveryPlaybook();
            ErrorContext ctx = new ErrorContext("UNKNOWN_ERROR", "something unexpected", Instant.now());
            assertThatCode(() -> playbook.handle("UNKNOWN_ERROR", ctx)).doesNotThrowAnyException();
        }
    }

    @Test
    void fullBootstrap_recoveryPlaybookAccessibleViaCoordinatorBootstrap() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("coord-recovery", 0,
                        CoordinatorBootstrapConfig.CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.server.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
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
                        CoordinatorBootstrapConfig.CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS, org.pangrova.server.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, false, false, false, false, true),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);
    }
}
