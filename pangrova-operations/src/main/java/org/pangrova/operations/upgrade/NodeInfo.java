package org.pangrova.operations.upgrade;

import java.util.Objects;

/**
 * Immutable descriptor of a cluster node participating in an upgrade.
 */
public record NodeInfo(String nodeId, NodeType type, String currentVersion) {

    public NodeInfo {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(currentVersion, "currentVersion");
    }

    public enum NodeType {
        WORKER,
        COORDINATOR
    }
}
