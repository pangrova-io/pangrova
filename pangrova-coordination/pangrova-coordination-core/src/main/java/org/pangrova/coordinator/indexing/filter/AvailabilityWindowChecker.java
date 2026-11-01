package org.pangrova.coordinator.indexing.filter;

import org.pangrova.indexing.planner.IndexHint;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;

import java.util.List;
import java.util.Map;

/**
 * Checks whether a file is outside the availability window of its indexes.
 * Files written after the last indexed snapshot bypass L1 evaluation (always KEEP).
 */
final class AvailabilityWindowChecker {

    private AvailabilityWindowChecker() {}

    static boolean isOutsideAvailabilityWindow(
            String dataFilePath, ConnectorSplit split,
            List<IndexHint> indexHints, SplitEvalContext ctx) {
        long fileSnapshotId = extractFileSnapshotId(split);
        if (fileSnapshotId < 0) {
            return false;
        }
        return indexHints.stream().anyMatch(hint -> {
            IndexDescriptor desc = ctx.descriptorsByIndex().get(hint.indexId());
            return desc != null && isAfterLastIndexed(fileSnapshotId, desc.lastIndexedSnapshotId());
        });
    }

    private static long extractFileSnapshotId(ConnectorSplit split) {
        Map<String, String> props = split.splitProperties();
        String fileSnapshotStr = props.get("iceberg.snapshot_id");
        if (fileSnapshotStr == null) {
            fileSnapshotStr = props.get("paimon.snapshot_id");
        }
        if (fileSnapshotStr == null) {
            return -1;
        }
        try {
            return Long.parseLong(fileSnapshotStr);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static boolean isAfterLastIndexed(long fileSnapshotId, String lastIndexedStr) {
        if (lastIndexedStr == null) {
            return false;
        }
        try {
            return fileSnapshotId > Long.parseLong(lastIndexedStr);
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
