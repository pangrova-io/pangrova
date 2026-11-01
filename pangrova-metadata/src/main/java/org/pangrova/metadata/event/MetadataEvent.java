package org.pangrova.metadata.event;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.table.api.control.OwnershipMode;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaChange;
import org.pangrova.metadata.api.catalog.MetadataChange;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Sealed interface for all metadata lifecycle events.
 */
public sealed interface MetadataEvent permits
        MetadataEvent.CatalogCreatedEvent,
        MetadataEvent.CatalogDroppedEvent,
        MetadataEvent.CatalogStateChangedEvent,
        MetadataEvent.TableCreatedEvent,
        MetadataEvent.TableDroppedEvent,
        MetadataEvent.TableAlteredEvent,
        MetadataEvent.MetadataSyncedEvent,
        MetadataEvent.SchemaEvolutionEvent,
        MetadataEvent.ExternalCommitObservedEvent {

    record CatalogCreatedEvent(CatalogDescriptor descriptor) implements MetadataEvent {}

    record CatalogDroppedEvent(String catalogName) implements MetadataEvent {}

    record CatalogStateChangedEvent(String catalogName, CatalogState oldState,
            CatalogState newState) implements MetadataEvent {}

    record TableCreatedEvent(QualifiedTableRef tableRef, TableMetadata metadata) implements MetadataEvent {}

    record TableDroppedEvent(QualifiedTableRef tableRef) implements MetadataEvent {}

    record TableAlteredEvent(QualifiedTableRef tableRef, List<SchemaChange> changes) implements MetadataEvent {
        /** Owns a stable change snapshot before asynchronous dispatch. */
        public TableAlteredEvent {
            changes = List.copyOf(changes);
        }
    }

    record MetadataSyncedEvent(String catalogName, MetadataChange change) implements MetadataEvent {}

    /**
     * A controlled table's version changed between observations. The writer may be
     * an external engine; this event records that Pangrova saw the change and must
     * not be treated as one of Pangrova's own commits.
     */
    record ExternalCommitObservedEvent(
            QualifiedTableRef tableRef,
            UUID tableId,
            OwnershipMode ownershipMode,
            String previousVersionToken,
            String versionToken,
            Instant observedAt) implements MetadataEvent {}

    /**
     * Dispatched when an ALTER-type schema evolution is detected during metadata sync.
     * Listeners (IndexCommitHandler, DynamicTableDdlExecutor) use this to mark
     * affected indexes for rebuild and DTs for schema refresh.
     */
    record TenantImpactScope(boolean allTenants, Set<String> tenantIds) {
        public TenantImpactScope {
            tenantIds = tenantIds == null ? Set.of() : Set.copyOf(tenantIds);
            if (allTenants == !tenantIds.isEmpty()) {
                throw new IllegalArgumentException(
                        "tenant impact must be either all tenants or a non-empty tenant set");
            }
        }

        public static TenantImpactScope global() {
            return new TenantImpactScope(true, Set.of());
        }

        public static TenantImpactScope tenants(Set<String> tenantIds) {
            return new TenantImpactScope(false, tenantIds);
        }
    }

    record SchemaEvolutionEvent(
            String catalogName,
            QualifiedTableRef tableRef,
            List<SchemaChange> schemaChanges,
            TenantImpactScope tenantScope) implements MetadataEvent {
        public SchemaEvolutionEvent {
            schemaChanges = List.copyOf(schemaChanges);
            if (tenantScope == null) {
                throw new IllegalArgumentException("tenantScope must not be null");
            }
        }
    }
}
