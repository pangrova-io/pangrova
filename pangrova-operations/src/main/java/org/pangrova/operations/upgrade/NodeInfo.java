package org.pangrova.operations.upgrade;

import org.pangrova.common.enums.ClusterNodeType;

import java.util.Objects;

/**
 * Immutable descriptor of a cluster node participating in an upgrade.
 */
public record NodeInfo(String nodeId, ClusterNodeType type, String currentVersion) {

    public NodeInfo {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(currentVersion, "currentVersion");
    }

}
