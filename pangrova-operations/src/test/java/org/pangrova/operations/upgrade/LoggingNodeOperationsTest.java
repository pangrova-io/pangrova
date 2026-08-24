package org.pangrova.operations.upgrade;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link LoggingNodeOperations} logs all lifecycle actions without error,
 * and that {@link RollingUpgradeController} can execute a full upgrade sequence
 * using the logging backend.
 */
class LoggingNodeOperationsTest {

    private final LoggingNodeOperations ops = new LoggingNodeOperations();

    @Test
    void allLifecycleMethodsExecuteWithoutError() {
        NodeInfo node = new NodeInfo("worker-1", NodeInfo.NodeType.WORKER, "1.0");
        ops.drain(node, Duration.ofSeconds(30));
        ops.waitTasks(node);
        ops.stop(node);
        ops.deploy(node, "1.1");
        ops.start(node);
        ops.verify(node);
        ops.undrain(node);
        ops.rollback(node);
    }

    @Test
    void rollingUpgradeControllerWithLoggingBackend() {
        VersionCompatibilityChecker checker = new VersionCompatibilityChecker(
                Map.of("1.0", 1, "1.1", 2));
        RollingUpgradeController controller = RollingUpgradeController.withLoggingBackend(
                checker, true, Duration.ofSeconds(5));

        List<NodeInfo> workers = List.of(
                new NodeInfo("w-1", NodeInfo.NodeType.WORKER, "1.0"));
        List<NodeInfo> coordinators = List.of(
                new NodeInfo("c-1", NodeInfo.NodeType.COORDINATOR, "1.0"));

        UpgradeResult result = controller.upgradeCluster(workers, coordinators, "1.1");
        assertThat(result.nodeResults()).hasSize(2);
        assertThat(result.nodeResults()).allMatch(NodeUpgradeResult::success);
    }
}
