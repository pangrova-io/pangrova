package org.pangrova.indexing.lifecycle;

import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageLayout;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.connector.spi.index.FileSystemAccessor;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Reacts to DDL events that change the identity of indexed tables.
 *
 * <p>Closes the gap between table-level DDL ({@code DROP TABLE}, {@code RENAME
 * TABLE}, {@code DROP INDEX}) and the index module: when a table disappears or
 * is renamed, the corresponding index data files in the index storage layout
 * become orphaned and their cache entries become stale. Without explicit
 * cleanup, orphaned bytes accumulate forever and a future
 * {@code CREATE TABLE} of the same name reads stale indexes.</p>
 *
 * <p>Listener semantics:
 * <ul>
 *   <li>{@link #onIndexDropped} — delete the index file from storage and
 *       invalidate the cache by indexId.</li>
 *   <li>{@link #onTableDropped} — delete every index file for the affected
 *       indexes and invalidate all cache entries for those indexIds.</li>
 *   <li>{@link #onTableRenamed} — re-key index files from the old logical
 *       table path to the new one; cache entries are invalidated because
 *       cache keys include the data-file path which changes after rename.</li>
 * </ul>
 *
 * <p>All operations are best-effort: a single failure (e.g., one inaccessible
 * file) is logged but does not abort the rest of the cleanup. The caller
 * (typically the coordinator's DDL executor or metadata event dispatcher)
 * remains responsible for sequencing this listener after the metadata
 * commit so that recovery from a partial cleanup is idempotent.</p>
 */
public final class IndexLifecycleListener {

    private static final Logger LOG = LoggerFactory.getLogger(IndexLifecycleListener.class);

    private final IndexCache indexCache;
    private final IndexStorageManager storageManager;
    private final IndexStorageLayout storageLayout;
    private final FileSystemAccessor fsAccessor;

    /** Wires the listener to the cache, storage manager, layout, and file system. */
    public IndexLifecycleListener(IndexCache indexCache,
                                  IndexStorageManager storageManager,
                                  IndexStorageLayout storageLayout,
                                  FileSystemAccessor fsAccessor) {
        this.indexCache = Objects.requireNonNull(indexCache, "indexCache");
        this.storageManager = Objects.requireNonNull(storageManager, "storageManager");
        this.storageLayout = Objects.requireNonNull(storageLayout, "storageLayout");
        this.fsAccessor = Objects.requireNonNull(fsAccessor, "fsAccessor");
    }

    /**
     * Cleans up after a {@code DROP INDEX}. Removes index files for each known
     * data-file mapping and invalidates the cache.
     */
    public void onIndexDropped(IndexDescriptor descriptor, List<String> dataFilePaths) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(dataFilePaths, "dataFilePaths");
        int deleteFailures = 0;
        for (String dataFilePath : dataFilePaths) {
            String indexFilePath = storageLayout.indexFilePath(descriptor, dataFilePath);
            if (!deleteIndexFileQuietly(descriptor.indexId(), indexFilePath)) {
                deleteFailures++;
            }
        }
        indexCache.invalidateIndex(descriptor.indexId());
        LOG.info("event=index_dropped_cleanup indexId={} files={} deleteFailures={}",
                descriptor.indexId(), dataFilePaths.size(), deleteFailures);
    }

    /**
     * Cleans up after a {@code DROP TABLE}. Removes index files for every
     * index attached to the table and invalidates each from the cache.
     */
    public void onTableDropped(List<IndexDescriptor> indexesOnTable,
                               List<String> dataFilePaths) {
        Objects.requireNonNull(indexesOnTable, "indexesOnTable");
        Objects.requireNonNull(dataFilePaths, "dataFilePaths");
        for (IndexDescriptor descriptor : indexesOnTable) {
            onIndexDropped(descriptor, dataFilePaths);
        }
    }

    /**
     * Cleans up after a {@code RENAME TABLE}. Invalidates the cache entries
     * keyed by old data-file paths so subsequent reads re-resolve to the new
     * paths. Physical index file relocation is the responsibility of the
     * coordinator's commit pipeline; this listener only ensures the in-memory
     * cache no longer hands out stale data.
     */
    public void onTableRenamed(List<IndexDescriptor> indexesOnTable,
                               List<String> oldDataFilePaths,
                               List<String> newDataFilePaths) {
        Objects.requireNonNull(indexesOnTable, "indexesOnTable");
        Objects.requireNonNull(oldDataFilePaths, "oldDataFilePaths");
        Objects.requireNonNull(newDataFilePaths, "newDataFilePaths");
        for (String oldPath : oldDataFilePaths) {
            indexCache.invalidateDataFile(oldPath);
        }
        LOG.info("event=table_renamed_cache_invalidated indexes={} oldFiles={} newFiles={}",
                indexesOnTable.size(), oldDataFilePaths.size(), newDataFilePaths.size());
    }

    private boolean deleteIndexFileQuietly(String indexId, String indexFilePath) {
        try {
            if (fsAccessor.exists(indexFilePath)) {
                fsAccessor.deleteFile(indexFilePath);
                LOG.debug("event=index_file_deleted indexId={} indexFile={}",
                        indexId, indexFilePath);
            }
            return true;
        } catch (Exception e) {
            LOG.warn("event=index_file_delete_failed indexId={} indexFile={} error={}",
                    indexId, indexFilePath, e.getMessage());
            return false;
        }
    }
}
