package org.pangrova.operations.upgrade;

import org.pangrova.common.enums.ClusterNodeType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RollingUpgradeControllerTest {
    private final List<String> calls = new ArrayList<>();
    private final NodeInfo worker = new NodeInfo("worker", ClusterNodeType.WORKER, "1.0");
    private final NodeInfo coordinator = new NodeInfo("coordinator", ClusterNodeType.COORDINATOR, "1.0");
    private boolean verificationFails;

    @Test
    void verifiesWorkersBeforeUpgradingTheCoordinator() {
        var result = controller().upgradeCluster(List.of(worker), List.of(coordinator), "1.1");
        assertThat(result.nodeResults()).hasSize(2).allMatch(NodeUpgradeResult::success);
        assertThat(calls).containsExactly("drain worker", "wait worker", "stop worker", "deploy 1.1 worker",
                "start worker", "verify worker", "undrain worker", "drain coordinator", "wait coordinator",
                "stop coordinator", "deploy 1.1 coordinator", "start coordinator", "verify coordinator",
                "undrain coordinator");
    }

    @Test
    void verificationFailureRollsBackAndStopsBeforeTouchingCoordinator() {
        verificationFails = true;
        var result = controller().upgradeCluster(List.of(worker), List.of(coordinator), "1.1");
        assertThat(result.nodeResults()).singleElement().satisfies(node -> {
            assertThat(node.success()).isFalse();
            assertThat(node.failureReason()).contains("verification failed");
        });
        assertThat(calls).containsExactly("drain worker", "wait worker", "stop worker", "deploy 1.1 worker",
                "start worker", "verify worker", "rollback worker");
    }

    private RollingUpgradeController controller() {
        return new RollingUpgradeController(new VersionCompatibilityChecker(Map.of("1.0", 1, "1.1", 2)),
                new NodeOperations() {
                    public void drain(NodeInfo node, Duration timeout) { calls.add("drain " + node.nodeId()); }
                    public void waitTasks(NodeInfo node) { calls.add("wait " + node.nodeId()); }
                    public void stop(NodeInfo node) { calls.add("stop " + node.nodeId()); }
                    public void deploy(NodeInfo node, String version) { calls.add("deploy " + version + " " + node.nodeId()); }
                    public void start(NodeInfo node) { calls.add("start " + node.nodeId()); }
                    public void verify(NodeInfo node) {
                        calls.add("verify " + node.nodeId());
                        if (verificationFails) {
                            throw new IllegalStateException("verification failed");
                        }
                    }
                    public void undrain(NodeInfo node) { calls.add("undrain " + node.nodeId()); }
                    public void rollback(NodeInfo node) { calls.add("rollback " + node.nodeId()); }
                }, true, Duration.ofSeconds(5));
    }
}
