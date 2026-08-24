package org.pangrova.operations.upgrade;

import java.util.List;
import java.util.Objects;

/**
 * Aggregate result of a cluster-wide rolling upgrade.
 */
public record UpgradeResult(List<NodeUpgradeResult> nodeResults) {

    public UpgradeResult {
        Objects.requireNonNull(nodeResults, "nodeResults");
        nodeResults = List.copyOf(nodeResults);
    }

    public boolean allSucceeded() {
        return nodeResults.stream().allMatch(r -> r.success());
    }
}
