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
     * @param session the real authenticated session carrying its issued query lifetime
     */
    void beginQuery(MetadataSessionContext session);

    /**
     * Cleans up query-level metadata resources across all catalogs.
     * Delegates to each registered connector's {@code cleanupQuery}.
     *
     * @param authority the same closed query lifetime retained during begin
     */
    void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority);

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

    /**
     * Removes every governance tag previously assigned to a table. Called when
     * the control record retires, so name-keyed policy cannot outlive it.
     *
     * @return the number of tag entries removed
     */
    default int clearTableTags(MetadataSessionContext session, QualifiedTableRef tableRef) {
        return 0;
    }

    /**
     * Moves every governance tag from one table name to another so name-keyed
     * policy follows an {@code ALTER TABLE ... RENAME}. Derived policy (for
     * example tag-based masks) is re-emitted at the new name and withdrawn from
     * the old one.
     *
     * @return the number of tag entries moved
     */
    default int renameTableTags(MetadataSessionContext session, QualifiedTableRef from,
                                QualifiedTableRef to) {
        return 0;
    }

}
