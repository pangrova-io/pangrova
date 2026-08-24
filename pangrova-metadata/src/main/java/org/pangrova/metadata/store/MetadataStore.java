package org.pangrova.metadata.store;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.sync.SyncResult;

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

    void deleteCatalogDescriptor(String name);

    List<CatalogDescriptor> listAllCatalogDescriptors();

    Optional<Instant> getLastSyncTime(String catalogName);

    void updateSyncTime(String catalogName, Instant syncTime);

    void recordSyncHistory(SyncResult result);

    void saveSchemas(String catalogName, List<String> schemas);
}
