package org.pangrova.metadata.dynamictable;

import java.time.Instant;
import java.util.Objects;

/** Durable definition and the currently published physical generation. */
public record DynamicTablePublication(
        String tenantId,
        String logicalName,
        String incarnationId,
        String definitionSql,
        String bindingCatalog,
        String bindingSchema,
        String publishedTable,
        long revision,
        State state,
        Instant createdAt,
        Instant updatedAt) {

    public enum State { CREATING, ACTIVE }

    public DynamicTablePublication {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(logicalName, "logicalName");
        Objects.requireNonNull(incarnationId, "incarnationId");
        Objects.requireNonNull(definitionSql, "definitionSql");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if (revision < 0 || (state == State.ACTIVE) != (publishedTable != null)) {
            throw new IllegalArgumentException("Invalid Dynamic Table publication state");
        }
    }
}
