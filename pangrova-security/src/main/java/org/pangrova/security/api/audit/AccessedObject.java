package org.pangrova.security.api.audit;

import java.util.List;

/**
 * Describes a database object accessed during a query, used in audit events.
 */
public record AccessedObject(
        String objectType,
        String catalog,
        String schema,
        String table,
        List<String> columns,
        String accessType
) {
    /**
     * Defensive copy of columns to ensure immutability.
     */
    public AccessedObject {
        columns = columns != null ? List.copyOf(columns) : List.of();
    }
}
