package org.pangrova.coordinator.indexing.lifecycle.gc;

import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.connector.spi.index.FileSystemAccessor;
import org.pangrova.connector.spi.index.SnapshotPinProvider;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;

import java.util.function.Function;

/**
 * Aggregated dependencies for index garbage collection (≤4 params).
 *
 * @param metadataStore       index metadata persistence
 * @param storageManager      index file I/O
 * @param fileAccessor        descriptor-scoped connector file accessor
 * @param config              indexing configuration
 * @param snapshotPinProvider snapshot pin provider (required when purging a snapshot-pinned index)
 */
public record IndexGcContext(
        IndexMetadataStore metadataStore,
        IndexStorageManager storageManager,
        Function<IndexDescriptor, FileSystemAccessor> fileAccessor,
        IndexingConfig config,
        SnapshotPinProvider snapshotPinProvider) {

    /**
     * Canonical constructor with snapshot pin provider.
     */
    public IndexGcContext {
        if (metadataStore == null) throw new IllegalArgumentException("metadataStore must not be null");
        if (storageManager == null) throw new IllegalArgumentException("storageManager must not be null");
        if (fileAccessor == null) throw new IllegalArgumentException("fileAccessor must not be null");
        if (config == null) throw new IllegalArgumentException("config must not be null");
    }

    /**
     * Backward-compatible constructor without snapshot pin provider.
     */
    public IndexGcContext(
            IndexMetadataStore metadataStore,
            IndexStorageManager storageManager,
            Function<IndexDescriptor, FileSystemAccessor> fileAccessor,
            IndexingConfig config) {
        this(metadataStore, storageManager, fileAccessor, config, null);
    }
}
