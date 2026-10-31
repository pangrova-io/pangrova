package org.pangrova.metadata.api.catalog;

import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Persistent store for metadata: tables, catalog descriptors, and sync tracking.
 */
public interface MetadataStore {

    void saveTable(TableMetadata metadata);

    Optional<TableMetadata> loadTable(String catalog, String schema, String table);

    void deleteTable(String catalog, String schema, String table);

    List<QualifiedTableRef> searchTables(String pattern);

    void saveCatalogDescriptor(CatalogDescriptor descriptor);

    Optional<CatalogDescriptor> getCatalogDescriptor(String name);

    /** Exact immutable deployment snapshot; never substitute the current descriptor. */
    default Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String name, String incarnationId, long revision) {
        return Optional.empty();
    }

    void deleteCatalogDescriptor(String name);

    List<CatalogDescriptor> listAllCatalogDescriptors();

    Optional<Instant> getLastSyncTime(String catalogName);

    void updateSyncTime(String catalogName, Instant syncTime);

    void recordSyncHistory(SyncResult result);

    void saveSchemas(String catalogName, List<String> schemas);
}
