package org.pangrova.metadata.event;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaChange;
import org.pangrova.metadata.sync.MetadataChange;

import java.util.List;

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
        MetadataEvent.SchemaEvolutionEvent {

    record CatalogCreatedEvent(CatalogDescriptor descriptor) implements MetadataEvent {}

    record CatalogDroppedEvent(String catalogName) implements MetadataEvent {}

    record CatalogStateChangedEvent(String catalogName, CatalogState oldState,
            CatalogState newState) implements MetadataEvent {}

    record TableCreatedEvent(QualifiedTableRef tableRef, TableMetadata metadata) implements MetadataEvent {}

    record TableDroppedEvent(QualifiedTableRef tableRef) implements MetadataEvent {}

    record TableAlteredEvent(QualifiedTableRef tableRef, List<SchemaChange> changes) implements MetadataEvent {}

    record MetadataSyncedEvent(String catalogName, MetadataChange change) implements MetadataEvent {}

    /**
     * Dispatched when an ALTER-type schema evolution is detected during metadata sync.
     * Listeners (IndexCommitHandler, DynamicTableDdlExecutor) use this to mark
     * affected indexes for rebuild and DTs for schema refresh.
     */
    record SchemaEvolutionEvent(String catalogName, QualifiedTableRef tableRef,
                                List<SchemaChange> schemaChanges) implements MetadataEvent {}
}
