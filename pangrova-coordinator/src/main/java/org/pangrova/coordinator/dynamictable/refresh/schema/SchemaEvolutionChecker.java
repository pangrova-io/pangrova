package org.pangrova.coordinator.dynamictable.refresh.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Checks schema evolution compatibility for Dynamic Table incremental refresh.
 *
 * <p>Compatibility rules:
 * <ul>
 *   <li>Column added → compatible (additive)</li>
 *   <li>Column removed → incompatible (breaking)</li>
 *   <li>Column type changed → incompatible (breaking)</li>
 *   <li>No changes → compatible</li>
 * </ul>
 */
public final class SchemaEvolutionChecker {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolutionChecker.class);

    /**
     * Check schema compatibility between the recorded schema and the current source schema.
     *
     * @param recordedSchema schema at DT creation/last redefine (column name → type)
     * @param currentSchema  current source table schema (column name → type)
     * @return check result with compatibility flag and detected changes
     */
    public SchemaCheckResult check(Map<String, String> recordedSchema,
                                   Map<String, String> currentSchema) {
        Objects.requireNonNull(recordedSchema, "recordedSchema must not be null");
        Objects.requireNonNull(currentSchema, "currentSchema must not be null");

        List<RefreshSchemaChange> changes = detectChanges(recordedSchema, currentSchema);
        if (changes.isEmpty()) {
            return SchemaCheckResult.noChanges();
        }
        boolean hasBreaking = hasBreakingChange(changes);
        logChanges(changes, hasBreaking);
        return hasBreaking
                ? SchemaCheckResult.incompatible(changes)
                : SchemaCheckResult.compatible(changes);
    }

    private List<RefreshSchemaChange> detectChanges(Map<String, String> recorded,
                                                    Map<String, String> current) {
        List<RefreshSchemaChange> changes = new ArrayList<>();
        detectRemovedAndChanged(recorded, current, changes);
        detectAdded(recorded, current, changes);
        return changes;
    }

    private void detectRemovedAndChanged(Map<String, String> recorded,
                                         Map<String, String> current,
                                         List<RefreshSchemaChange> changes) {
        for (var entry : recorded.entrySet()) {
            String col = entry.getKey();
            String oldType = entry.getValue();
            String newType = current.get(col);
            if (newType == null) {
                changes.add(new RefreshSchemaChange.ColumnRemoved(col));
            } else if (!oldType.equals(newType)) {
                changes.add(new RefreshSchemaChange.ColumnTypeChanged(col, oldType, newType));
            }
        }
    }

    private void detectAdded(Map<String, String> recorded,
                             Map<String, String> current,
                             List<RefreshSchemaChange> changes) {
        for (var entry : current.entrySet()) {
            if (!recorded.containsKey(entry.getKey())) {
                changes.add(new RefreshSchemaChange.ColumnAdded(entry.getKey(), entry.getValue()));
            }
        }
    }

    private boolean hasBreakingChange(List<RefreshSchemaChange> changes) {
        return changes.stream().anyMatch(c ->
                c instanceof RefreshSchemaChange.ColumnRemoved
                        || c instanceof RefreshSchemaChange.ColumnTypeChanged);
    }

    private void logChanges(List<RefreshSchemaChange> changes, boolean hasBreaking) {
        LOG.info("event=dt_schema_check changeCount={} hasBreaking={}", changes.size(), hasBreaking);
    }
}
