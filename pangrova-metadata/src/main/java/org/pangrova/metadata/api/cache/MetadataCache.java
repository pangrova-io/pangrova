package org.pangrova.metadata.api.cache;

import org.pangrova.metadata.api.object.TableMetadata;

import java.util.Optional;

/**
 * Cache interface for table metadata lookups.
 */
public interface MetadataCache {

    Optional<TableMetadata> getTable(String catalog, String schema, String table);

    void putTable(String catalog, String schema, String table, TableMetadata metadata);

    void invalidateTable(String catalog, String schema, String table);

    void invalidateCatalog(String catalog);

    CacheStats getStats();
}
