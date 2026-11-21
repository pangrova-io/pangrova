package org.pangrova.operations.upgrade;

import org.pangrova.common.annotation.ExperimentalImplementation;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Orchestrates rolling upgrades across cluster nodes.
 * Workers are upgraded first, then coordinators.
 *
 * <p>The host supplies a {@link NodeOperations} backend that completes each
 * infrastructure operation or throws. No infrastructure backend ships with this controller.</p>
 */
@ExperimentalImplementation(trackingIssue = "https://github.com/pangrova/pangrova/issues/F-OPS-002-rolling-upgrade")
public final class RollingUpgradeController {

    private static final Logger LOG = LoggerFactory.getLogger(RollingUpgradeController.class);

    private final VersionCompatibilityChecker compatibilityChecker;
    private final NodeOperations nodeOps;
    private final boolean rollbackOnFailure;
    private final Duration drainTimeout;

    /** Creates a controller with the given backend and failure policy. */
    public RollingUpgradeController(
            VersionCompatibilityChecker compatibilityChecker,
            NodeOperations nodeOps,
            boolean rollbackOnFailure,
            Duration drainTimeout) {
        this.compatibilityChecker = Objects.requireNonNull(compatibilityChecker);
        this.nodeOps = Objects.requireNonNull(nodeOps);
        this.rollbackOnFailure = rollbackOnFailure;
        this.drainTimeout = Objects.requireNonNull(drainTimeout);
    }

    /** Upgrades all workers first, then coordinators, returning per-node results. */
    public UpgradeResult upgradeCluster(
            List<NodeInfo> workers,
            List<NodeInfo> coordinators,
            String targetVersion) {
        List<NodeUpgradeResult> results = new ArrayList<>();
        boolean aborted = upgradeNodes(workers, targetVersion, results);
        if (!aborted) {
            upgradeNodes(coordinators, targetVersion, results);
        }
        return new UpgradeResult(results);
    }

    /**
     * Upgrades nodes sequentially. Stops on first failure when rollbackOnFailure is enabled.
     *
     * @return true if upgrade was aborted due to a node failure
     */
    private boolean upgradeNodes(
            List<NodeInfo> nodes,
            String targetVersion,
            List<NodeUpgradeResult> results) {
        for (int i = 0; i < nodes.size(); i++) {
            NodeInfo node = nodes.get(i);
            NodeUpgradeResult result = upgradeNode(node, targetVersion);
            results.add(result);
            if (!result.success() && rollbackOnFailure) {
                LOG.warn("event=upgrade_aborted_on_failure nodeId={} remainingNodes={}",
                        node.nodeId(), nodes.size() - i - 1);
                return true;
            }
        }
        return false;
    }

    NodeUpgradeResult upgradeNode(NodeInfo node, String targetVersion) {
        if (!compatibilityChecker.isCompatible(node.currentVersion(), targetVersion)) {
            return NodeUpgradeResult.failed(node,
                    new PangrovaException(PangrovaErrorCode.OPS_VERSION_INCOMPATIBLE,
                            "Incompatible: " + node.currentVersion() + " -> " + targetVersion));
        }
        try {
            executePhases(node, targetVersion);
            return NodeUpgradeResult.success(node);
        } catch (Exception e) {
            LOG.error("event=node_upgrade_failed nodeId={} error={}", node.nodeId(), e.getMessage());
            return handleFailure(node, e);
        }
    }

    private void executePhases(NodeInfo node, String targetVersion) {
        nodeOps.drain(node, drainTimeout);
        nodeOps.waitTasks(node);
        nodeOps.stop(node);
        nodeOps.deploy(node, targetVersion);
        nodeOps.start(node);
        nodeOps.verify(node);
        nodeOps.undrain(node);
    }

    private NodeUpgradeResult handleFailure(NodeInfo node, Exception cause) {
        if (rollbackOnFailure) {
            tryRollback(node);
        }
        return NodeUpgradeResult.failed(node, cause);
    }

    private void tryRollback(NodeInfo node) {
        try {
            nodeOps.rollback(node);
        } catch (Exception rollbackEx) {
            LOG.error("event=node_rollback_failed nodeId={} error={}", node.nodeId(), rollbackEx.getMessage());
        }
    }
}
