package org.pangrova.metadata.core.cache;

import org.pangrova.metadata.api.schema.TableMetadata;

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
