package org.pangrova.spi.connector.materialization;

import org.pangrova.common.api.annotation.Stable;

/**
 * Versioned capability declaration of a materialization source connector.
 *
 * <p>Capabilities are explicit and fail closed. A missing capability means the
 * kernel must reject the corresponding operation; it must never guess or
 * silently downgrade to a weaker execution mode. {@code changeSet} requires
 * {@code snapshotPin} because an incremental change-set is only meaningful
 * between two pinned source snapshots.
 *
 * @param version capability format version understood by the connector
 * @param snapshotPin connector can pin a source snapshot for the duration of an attempt
 * @param appendOnlyProof connector can prove that a change interval contains only appends
 * @param changeSet connector can enumerate changes between two pinned snapshots
 * @param partitionReplace connector can identify affected partitions for replacement
 * @param affectedKey connector can identify affected deterministic aggregation keys
 * @param cdc connector can emit an ordered change-data stream
 * @param internalNamespace connector can create physical materializations in the internal namespace
 * @param idempotentCommitLookup connector can look up a commit by its exact fingerprint after a crash
 */
@Stable(since = "1.0")
public record MaterializationCapabilities(
        int version,
        boolean snapshotPin,
        boolean appendOnlyProof,
        boolean changeSet,
        boolean partitionReplace,
        boolean affectedKey,
        boolean cdc,
        boolean internalNamespace,
        boolean idempotentCommitLookup) {

    /** Capability format version this kernel understands. */
    public static final int CURRENT_VERSION = 1;

    public MaterializationCapabilities {
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        if (changeSet && !snapshotPin) {
            throw new IllegalArgumentException("changeSet requires snapshotPin");
        }
    }

    /**
     * Returns whether this connector can serve the requested change-set mode.
     *
     * @param mode requested change-set mode
     * @return true when every capability required by the mode is present
     */
    public boolean supports(ChangeSetMode mode) {
        return switch (mode) {
            case APPEND_ONLY -> snapshotPin && appendOnlyProof && changeSet;
            case PARTITION_REPLACE -> snapshotPin && changeSet && partitionReplace;
            case AFFECTED_KEY -> snapshotPin && changeSet && affectedKey;
            case CDC -> snapshotPin && cdc;
        };
    }

    /**
     * Rejects capability descriptors produced by an incompatible kernel version.
     *
     * @throws IllegalStateException when the descriptor version is unknown
     */
    public void requireCompatibleVersion() {
        if (version != CURRENT_VERSION) {
            throw new IllegalStateException(
                    "Unsupported materialization capability version " + version
                            + "; expected " + CURRENT_VERSION);
        }
    }
}
