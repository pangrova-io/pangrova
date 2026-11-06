package org.pangrova.table.api.maintenance;

import java.util.Locale;
import java.util.Optional;

/**
 * Maintenance actions the control plane can authorize for a controlled table.
 *
 * <p>{@link #OPTIMIZE} is a user-facing alias for compaction and is admitted
 * whenever {@link #COMPACT} is enabled. {@link #VACUUM} is the orphan-file
 * cleanup action and is admitted whenever {@link #REMOVE_ORPHAN_FILES} is
 * enabled.</p>
 */
public enum MaintenanceOperation {
    COMPACT,
    OPTIMIZE,
    VACUUM,
    EXPIRE_SNAPSHOTS,
    REWRITE_MANIFESTS,
    REMOVE_ORPHAN_FILES;

    /** Parses a policy or SQL value, returning empty for unknown names. */
    public static Optional<MaintenanceOperation> parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(valueOf(raw.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException unknown) {
            return Optional.empty();
        }
    }

    /** Compaction is a single action even though it has two user-facing names. */
    public boolean isCompaction() {
        return this == COMPACT || this == OPTIMIZE;
    }

    /** Orphan cleanup is a single action even though it has two user-facing names. */
    public boolean isOrphanCleanup() {
        return this == VACUUM || this == REMOVE_ORPHAN_FILES;
    }
}
