package org.pangrova.connector.api.index;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * SPI for managing snapshot pins that prevent compaction from deleting
 * snapshots still referenced by active indexes.
 *
 * <p>Snapshot tokens are opaque connector values. Implementations may interpret
 * their own tokens but must round-trip them without numeric coercion. Lifecycle
 * operations are idempotent for the exact {@link SnapshotPinOwner}.</p>
 */
@Experimental
public interface SnapshotPinProvider {

    /** Pins one immutable snapshot before any source inventory or row read. */
    void pinSnapshot(String catalogName,
                     String schemaName,
                     String tableName,
                     String snapshotToken,
                     SnapshotPinOwner owner);

    /** Releases exactly one owner's pin; a missing matching pin is a successful no-op. */
    void unpinSnapshot(String catalogName,
                       String schemaName,
                       String tableName,
                       String snapshotToken,
                       SnapshotPinOwner owner);

    /** Fails closed unless the exact owner's pin still resolves to the requested snapshot. */
    void validateSnapshotPin(String catalogName,
                             String schemaName,
                             String tableName,
                             String snapshotToken,
                             SnapshotPinOwner owner);

    /** Returns every distinct opaque snapshot token currently pinned for the table. */
    List<String> getPinnedSnapshots(String catalogName,
                                    String schemaName,
                                    String tableName);

    /** Enumerates exact durable pins for one index, including retired generations. */
    List<SnapshotPinReference> getSnapshotPinsForIndex(String catalogName, String schemaName,
                                                       String tableName, String indexId);
}
