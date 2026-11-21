package org.pangrova.indexing.pruning;

import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.indexing.api.pruning.IndexHint;

import java.util.List;

/** Checks split snapshot evidence before using a published scalar index. */
final class AvailabilityWindowChecker {
    private AvailabilityWindowChecker() {}

    static boolean isOutsideAvailabilityWindow(
            ConnectorSplit split, List<IndexHint> hints, SplitEvalContext context) {
        var properties = split.splitProperties();
        String snapshot = properties.getOrDefault("iceberg.snapshot_id", properties.get("paimon.snapshot_id"));
        if (snapshot == null) {
            return false;
        }
        if (context.querySnapshotToken() != null) {
            return !snapshot.equals(context.querySnapshotToken());
        }
        return hints.stream().anyMatch(hint -> {
            var descriptor = context.descriptorsByIndex().get(hint.indexId());
            return descriptor == null || !snapshot.equals(descriptor.lastIndexedSnapshotId());
        });
    }
}
