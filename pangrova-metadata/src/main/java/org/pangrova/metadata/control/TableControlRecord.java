package org.pangrova.metadata.control;

import org.pangrova.metadata.model.QualifiedTableRef;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Durable management state for a LINKED or MANAGED table.
 *
 * <p>{@code tableId} is the stable identity. It is intentionally independent of
 * {@code catalog.schema.table}: dropping and recreating a table under the same
 * name produces a different identity, so stale writes and maintenance intent
 * cannot silently target the new table.</p>
 *
 * <p>{@code metadataRevision} increments on every control mutation and is the
 * optimistic-concurrency token for active-active coordinators.</p>
 */
public record TableControlRecord(
        UUID tableId,
        String catalog,
        String schema,
        String table,
        String format,
        String formatTableUuid,
        String formatLocation,
        OwnershipMode ownershipMode,
        TableControlState state,
        TableControlProvenance provenance,
        String catalogIncarnationId,
        Long catalogRevision,
        long metadataRevision,
        String versionToken,
        Instant versionObservedAt,
        String ownerPrincipal,
        Map<String, String> properties,
        String sourceSignature,
        Instant createdAt,
        Instant updatedAt,
        Instant lastSyncedAt,
        Instant lastMaintainedAt) {

    public TableControlRecord {
        Objects.requireNonNull(tableId, "tableId");
        catalog = requireNonBlank(catalog, "catalog");
        schema = requireNonBlank(schema, "schema");
        table = requireNonBlank(table, "table");
        format = requireNonBlank(format, "format");
        Objects.requireNonNull(ownershipMode, "ownershipMode");
        if (ownershipMode == OwnershipMode.OBSERVED) {
            throw new IllegalArgumentException("OBSERVED tables are never persisted");
        }
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(provenance, "provenance");
        if (metadataRevision < 0) {
            throw new IllegalArgumentException("metadataRevision must not be negative");
        }
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    public QualifiedTableRef tableRef() {
        return new QualifiedTableRef(catalog, schema, table);
    }

    public String qualifiedName() {
        return catalog + "." + schema + "." + table;
    }

    /** Returns true when writes, destructive DDL, and maintenance may proceed. */
    public boolean isUsable() {
        return state == TableControlState.ACTIVE && ownershipMode == OwnershipMode.MANAGED;
    }

    /** Returns a builder pre-populated from this record for a copy-with change. */
    public Builder toBuilder() {
        return new Builder()
                .tableId(tableId)
                .name(catalog, schema, table)
                .format(format)
                .formatTableUuid(formatTableUuid)
                .formatLocation(formatLocation)
                .ownershipMode(ownershipMode)
                .state(state)
                .provenance(provenance)
                .catalogIncarnationId(catalogIncarnationId)
                .catalogRevision(catalogRevision)
                .metadataRevision(metadataRevision)
                .version(versionToken, versionObservedAt)
                .ownerPrincipal(ownerPrincipal)
                .properties(properties)
                .sourceSignature(sourceSignature)
                .createdAt(createdAt)
                .updatedAt(updatedAt)
                .lastSyncedAt(lastSyncedAt)
                .lastMaintainedAt(lastMaintainedAt);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder; a record has too many components for positional construction. */
    public static final class Builder {
        private UUID tableId;
        private String catalog;
        private String schema;
        private String table;
        private String format;
        private String formatTableUuid;
        private String formatLocation;
        private OwnershipMode ownershipMode;
        private TableControlState state;
        private TableControlProvenance provenance;
        private String catalogIncarnationId;
        private Long catalogRevision;
        private long metadataRevision;
        private String versionToken;
        private Instant versionObservedAt;
        private String ownerPrincipal = "";
        private Map<String, String> properties = Map.of();
        private String sourceSignature;
        private Instant createdAt;
        private Instant updatedAt;
        private Instant lastSyncedAt;
        private Instant lastMaintainedAt;

        public Builder tableId(UUID value) {
            this.tableId = value;
            return this;
        }

        /** Positions the builder at a catalog.schema.table name. */
        public Builder name(String catalogName, String schemaName, String tableName) {
            this.catalog = catalogName;
            this.schema = schemaName;
            this.table = tableName;
            return this;
        }

        public Builder format(String value) {
            this.format = value;
            return this;
        }

        public Builder formatTableUuid(String value) {
            this.formatTableUuid = value;
            return this;
        }

        public Builder formatLocation(String value) {
            this.formatLocation = value;
            return this;
        }

        public Builder ownershipMode(OwnershipMode value) {
            this.ownershipMode = value;
            return this;
        }

        public Builder state(TableControlState value) {
            this.state = value;
            return this;
        }

        public Builder provenance(TableControlProvenance value) {
            this.provenance = value;
            return this;
        }

        public Builder catalogIncarnationId(String value) {
            this.catalogIncarnationId = value;
            return this;
        }

        public Builder catalogRevision(Long value) {
            this.catalogRevision = value;
            return this;
        }

        public Builder metadataRevision(long value) {
            this.metadataRevision = value;
            return this;
        }

        public Builder version(String token, Instant observedAt) {
            this.versionToken = token;
            this.versionObservedAt = observedAt;
            return this;
        }

        public Builder ownerPrincipal(String value) {
            this.ownerPrincipal = value == null ? "" : value;
            return this;
        }

        public Builder properties(Map<String, String> value) {
            this.properties = value == null ? Map.of() : Map.copyOf(value);
            return this;
        }

        public Builder sourceSignature(String value) {
            this.sourceSignature = value;
            return this;
        }

        public Builder createdAt(Instant value) {
            this.createdAt = value;
            return this;
        }

        public Builder updatedAt(Instant value) {
            this.updatedAt = value;
            return this;
        }

        public Builder lastSyncedAt(Instant value) {
            this.lastSyncedAt = value;
            return this;
        }

        public Builder lastMaintainedAt(Instant value) {
            this.lastMaintainedAt = value;
            return this;
        }

        /** Builds the immutable record from the current builder state. */
        public TableControlRecord build() {
            return new TableControlRecord(
                    tableId, catalog, schema, table, format, formatTableUuid, formatLocation,
                    ownershipMode, state, provenance, catalogIncarnationId, catalogRevision,
                    metadataRevision, versionToken, versionObservedAt, ownerPrincipal,
                    properties, sourceSignature, createdAt, updatedAt, lastSyncedAt,
                    lastMaintainedAt);
        }
    }
}
