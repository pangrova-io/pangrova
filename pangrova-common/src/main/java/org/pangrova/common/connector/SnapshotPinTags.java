package org.pangrova.common.connector;

import org.pangrova.common.util.DigestUtils;

/** Persistent tag names shared by connectors that pin snapshots for a Pangrova index. */
public final class SnapshotPinTags {
    public static final String PREFIX = "pangrova_index_";

    private SnapshotPinTags() {}

    /** Returns the namespace for all snapshots held by this exact index identity. */
    public static String indexPrefix(String indexId) {
        if (indexId == null || indexId.isBlank()) {
            throw new IllegalArgumentException("Index ID is required for snapshot pinning");
        }
        return PREFIX + DigestUtils.sha256Hex(indexId);
    }

    public static String forSnapshot(String indexId, long snapshotId) {
        return indexPrefix(indexId) + "_" + snapshotId;
    }
}
