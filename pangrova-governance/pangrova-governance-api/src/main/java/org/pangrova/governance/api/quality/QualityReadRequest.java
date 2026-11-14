package org.pangrova.governance.api.quality;

import java.util.Objects;
import java.util.UUID;

/** A rule read is structured data; only the query adapter may parse its expression and construct SQL. */
public record QualityReadRequest(String catalog, String schema, String table, UUID tableId, String expression,
                                 GovernanceQueryRunner.QueryType type) {
    public QualityReadRequest {
        requireName(catalog, "catalog");
        requireName(schema, "schema");
        requireName(table, "table");
        Objects.requireNonNull(tableId, "Stable table ID is required");
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("Quality expression is required");
        }
        Objects.requireNonNull(type, "type");
    }

    private static void requireName(String name, String part) {
        if (name == null || name.isBlank() || name.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Quality " + part + " identifier is required and cannot contain NUL");
        }
    }
}
