package org.pangrova.coordinator.dynamictable.refresh.schema;

import java.util.List;
import java.util.Objects;

/**
 * Result of a schema evolution compatibility check.
 *
 * @param compatible whether the schema change is compatible with incremental refresh
 * @param changes    list of detected schema changes
 */
public record SchemaCheckResult(
        boolean compatible,
        List<RefreshSchemaChange> changes
) {
    public SchemaCheckResult {
        Objects.requireNonNull(changes, "changes must not be null");
        changes = List.copyOf(changes);
    }

    /** No changes detected — fully compatible. */
    public static SchemaCheckResult noChanges() {
        return new SchemaCheckResult(true, List.of());
    }

    /** Compatible changes (only column additions). */
    public static SchemaCheckResult compatible(List<RefreshSchemaChange> changes) {
        return new SchemaCheckResult(true, changes);
    }

    /** Incompatible changes (column removal or type change). */
    public static SchemaCheckResult incompatible(List<RefreshSchemaChange> changes) {
        return new SchemaCheckResult(false, changes);
    }
}
