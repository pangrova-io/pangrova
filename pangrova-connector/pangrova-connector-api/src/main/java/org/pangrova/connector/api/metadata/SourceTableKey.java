package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

/** Tenant-scoped SQL identity, independent of a storage format. */
@Experimental
public record SourceTableKey(String tenant, SchemaTableName table) {
    public SourceTableKey {
        text(tenant);
        java.util.Objects.requireNonNull(table);
        text(table.schema());
        text(table.table());
    }
    private static void text(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Source identity is required");
    }
}
