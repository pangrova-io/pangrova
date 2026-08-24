package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.spi.connector.index.SnapshotPinProvider;

/**
 * Aggregated dependencies for index garbage collection (≤4 params).
 *
 * @param metadataStore       index metadata persistence
 * @param storageManager      index file I/O
 * @param accessorResolver    connector accessor resolution
 * @param config              indexing configuration
 * @param snapshotPinProvider snapshot pin provider (optional, null disables unpinning)
 */
public record IndexGcContext(
        IndexMetadataStore metadataStore,
        IndexStorageManager storageManager,
        ConnectorAccessorResolver accessorResolver,
        IndexingConfig config,
        SnapshotPinProvider snapshotPinProvider) {

    /**
     * Canonical constructor with snapshot pin provider.
     */
    public IndexGcContext {
        if (metadataStore == null) throw new IllegalArgumentException("metadataStore must not be null");
        if (storageManager == null) throw new IllegalArgumentException("storageManager must not be null");
        if (accessorResolver == null) throw new IllegalArgumentException("accessorResolver must not be null");
        if (config == null) throw new IllegalArgumentException("config must not be null");
    }

    /**
     * Backward-compatible constructor without snapshot pin provider.
     */
    public IndexGcContext(
            IndexMetadataStore metadataStore,
            IndexStorageManager storageManager,
            ConnectorAccessorResolver accessorResolver,
            IndexingConfig config) {
        this(metadataStore, storageManager, accessorResolver, config, null);
    }
}
