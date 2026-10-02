package org.pangrova.metadata.service;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.schema.SchemaChange;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Top-level metadata operations: catalog lifecycle, schema browsing, table resolution, and DDL.
 */
public interface MetadataService {

    void createCatalog(MetadataSessionContext session, CatalogDescriptor descriptor);

    void dropCatalog(MetadataSessionContext session, String catalogName, boolean cascade);

    List<String> listCatalogs(MetadataSessionContext session);

    /** Returns the unresolved authoritative catalog descriptor for internal deployment. */
    default Optional<CatalogDescriptor> getCatalogDescriptor(String catalogName) {
        return Optional.empty();
    }

    /** Resolves only the requested durable catalog incarnation and revision. */
    default Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String catalogName, String incarnationId, long revision) {
        return Optional.empty();
    }

    List<String> listSchemas(MetadataSessionContext session, String catalogName);

    ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef);

    /**
     * Creates a new table and emits a {@code TableCreatedEvent} to downstream consumers.
     *
     * @param session  metadata session context
     * @param tableRef qualified table reference (catalog.schema.table)
     * @param metadata table metadata including columns, partitioning, and properties
     */
    void createTable(MetadataSessionContext session, QualifiedTableRef tableRef, TableMetadata metadata);

    /**
     * Drops an existing table and emits a {@code TableDroppedEvent} to downstream consumers.
     *
     * @param session  metadata session context
     * @param tableRef qualified table reference (catalog.schema.table)
     */
    void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef);

    void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes);

    /**
     * Begins a query-level metadata transaction across all catalogs.
     * Delegates to each registered connector's {@code beginQuery}.
     *
     * @param queryId the unique query identifier
     */
    void beginQuery(String queryId);

    /**
     * Cleans up query-level metadata resources across all catalogs.
     * Delegates to each registered connector's {@code cleanupQuery}.
     *
     * @param queryId the unique query identifier
     */
    void cleanupQuery(String queryId);

    // ── Column governance tags (delegated to ColumnTagService) ──

    /**
     * Assigns a governance tag to a column. Emits a {@code TagChangedEvent}
     * so listeners (e.g. TagBasedSecurityPolicy) can activate auto-masking
     * or audit policies.
     *
     * <p>Default implementation throws {@link UnsupportedOperationException}
     * — implementations that do not wire a ColumnTagService can keep it.
     *
     * @param session    metadata session context
     * @param tableRef   qualified table reference
     * @param columnName column name receiving the tag
     * @param tagKey     tag key being assigned
     * @param value      tag value
     */
    default void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                        String columnName, String tagKey, String value) {
        throw new UnsupportedOperationException(
                "setTag not supported by this MetadataService implementation");
    }

    /**
     * Removes a previously assigned governance tag from a column. Emits a
     * {@code TagChangedEvent} on success.
     *
     * @param session    metadata session context
     * @param tableRef   qualified table reference
     * @param columnName column name losing the tag
     * @param tagKey     tag key being removed
     */
    default void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                          String columnName, String tagKey) {
        throw new UnsupportedOperationException(
                "unsetTag not supported by this MetadataService implementation");
    }

    /**
     * Lists all governance tags currently assigned to a column.
     *
     * @param session    metadata session context
     * @param tableRef   qualified table reference
     * @param columnName column name to inspect
     * @return map of tagKey → value (may be empty); never null
     */
    default Map<String, String> listTags(MetadataSessionContext session,
                                         QualifiedTableRef tableRef, String columnName) {
        return Map.of();
    }

}
