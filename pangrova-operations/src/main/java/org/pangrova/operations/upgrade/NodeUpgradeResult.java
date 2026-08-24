package org.pangrova.operations.upgrade;

import java.util.Objects;

/**
 * Result of upgrading a single node.
 */
public record NodeUpgradeResult(NodeInfo node, boolean success, String failureReason) {

    public NodeUpgradeResult {
        Objects.requireNonNull(node, "node");
    }

    public static NodeUpgradeResult success(NodeInfo node) {
        return new NodeUpgradeResult(node, true, null);
    }

    public static NodeUpgradeResult failed(NodeInfo node, Exception cause) {
        String reason = cause != null ? cause.getMessage() : "unknown";
        return new NodeUpgradeResult(node, false, reason);
    }

    public static NodeUpgradeResult aborted(NodeInfo node, Exception cause) {
        String reason = "aborted: " + (cause != null ? cause.getMessage() : "unknown");
        return new NodeUpgradeResult(node, false, reason);
    }
}
