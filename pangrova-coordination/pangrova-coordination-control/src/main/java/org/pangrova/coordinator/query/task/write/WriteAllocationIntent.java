package org.pangrova.coordinator.query.task.write;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Exact immutable SQL allocation intent; authority alone does not identify append/overwrite or partitions. */
public record WriteAllocationIntent(String sqlWriteMode, Map<String, String> staticPartitions) {
    public WriteAllocationIntent {
        if (!Set.of("INSERT", "OVERWRITE", "UPDATE", "DELETE", "MERGE").contains(sqlWriteMode)) {
            throw new IllegalArgumentException("Unsupported SQL write allocation mode: " + sqlWriteMode);
        }
        var canonical = new TreeMap<>(Objects.requireNonNull(staticPartitions, "staticPartitions"));
        canonical.forEach((name, value) -> {
            if (name.isBlank() || value == null) {
                throw new IllegalArgumentException("Static partitions require nonempty names and exact values");
            }
        });
        staticPartitions = Map.copyOf(canonical);
        if (sqlWriteMode.equals("INSERT") && !staticPartitions.isEmpty()) {
            throw new IllegalArgumentException("Append allocation cannot carry overwrite partitions");
        }
    }
}
