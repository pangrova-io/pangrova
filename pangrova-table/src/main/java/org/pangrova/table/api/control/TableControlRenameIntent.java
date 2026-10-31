package org.pangrova.table.api.control;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Durable source-name reservation for one admitted physical rename. */
public final class TableControlRenameIntent {
    private static final String PREFIX = "control.rename.";

    private TableControlRenameIntent() {}

    /** Returns the original reserved source while a physical rename is unresolved. */
    public static Optional<TableRef> source(TableControlRecord record) {
        if (record.state() != TableControlState.RENAMING) return Optional.empty();
        var properties = record.properties();
        if (!properties.containsKey(PREFIX + "operation")) {
            throw new IllegalStateException("RENAMING record has no durable source intent: " + record.tableId());
        }
        return Optional.of(new TableRef(properties.get(PREFIX + "catalog"),
                properties.get(PREFIX + "schema"), properties.get(PREFIX + "table")));
    }

    /** Captures one operation identity and its original source before physical mutation. */
    public static Map<String, String> begin(TableControlRecord record) {
        if (record.state() == TableControlState.RENAMING) {
            source(record); // Never replace the original source during recovery.
            return record.properties();
        }
        var properties = new LinkedHashMap<>(record.properties());
        properties.put(PREFIX + "operation", UUID.randomUUID().toString());
        properties.put(PREFIX + "catalog", record.catalog());
        properties.put(PREFIX + "schema", record.schema());
        properties.put(PREFIX + "table", record.table());
        return Map.copyOf(properties);
    }

    /** Releases technical intent only after a proved successful completion. */
    public static Map<String, String> complete(TableControlRecord record) {
        var properties = new LinkedHashMap<>(record.properties());
        properties.keySet().removeIf(key -> key.startsWith(PREFIX));
        return Map.copyOf(properties);
    }
}
