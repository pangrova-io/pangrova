package org.pangrova.metadata.dynamictable;

import java.util.Objects;

/** Immutable logical declaration submitted to atomic definition/first-attempt reservation. */
public record DynamicTableDefinition(String tenantId, String logicalName, String definitionSql,
                                     String bindingCatalog, String bindingSchema) {
    public DynamicTableDefinition {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(logicalName, "logicalName");
        Objects.requireNonNull(definitionSql, "definitionSql");
    }
}
