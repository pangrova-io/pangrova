package org.pangrova.metadata.core.cache;

import org.pangrova.metadata.api.schema.TableMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Test-only {@link MetadataCache} that records all invalidation calls for assertion.
 * No actual caching — only tracks what was invalidated.
 */
final class RecordingMetadataCache implements MetadataCache {

    final List<TableKey> tableInvalidations = new ArrayList<>();
    final List<String> catalogInvalidations = new ArrayList<>();

    record TableKey(String catalog, String schema, String table) {}

    @Override
    public Optional<TableMetadata> getTable(String catalog, String schema, String table) {
        return Optional.empty();
    }

    @Override
    public void putTable(String catalog, String schema, String table, TableMetadata metadata) {
        // no-op for testing
    }

    @Override
    public void invalidateTable(String catalog, String schema, String table) {
        tableInvalidations.add(new TableKey(catalog, schema, table));
    }

    @Override
    public void invalidateCatalog(String catalog) {
        catalogInvalidations.add(catalog);
    }

    @Override
    public CacheStats getStats() {
        return new CacheStats(0, 0, 0, 0);
    }
}
