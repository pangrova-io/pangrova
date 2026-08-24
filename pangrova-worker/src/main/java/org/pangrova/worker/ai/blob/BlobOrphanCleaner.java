package org.pangrova.worker.ai.blob;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Scans blob storage prefixes and deletes unreferenced blob files.
 *
 * <p>Blob files live at {@code {table_location}/blob/{col}/{uuid}.{ext}}.
 * A blob is considered orphaned if its URI is not referenced by any data file
 * in the table and it is older than the configured grace period (default 24 hours).
 * The grace period prevents deletion of blobs that are being written concurrently
 * but not yet committed.</p>
 *
 * <p>Thread safety: instances are safe for concurrent use. The cleanup operation
 * itself is stateless; all mutable state is local to each invocation.</p>
 */
@ThreadSafe
public final class BlobOrphanCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(BlobOrphanCleaner.class);

    static final Duration DEFAULT_GRACE_PERIOD = Duration.ofHours(24);

    private final BlobStorageScanner storageScanner;
    private final Clock clock;

    /**
     * Creates a cleaner with the system clock.
     *
     * @param storageScanner scanner for listing and deleting blob files
     */
    public BlobOrphanCleaner(BlobStorageScanner storageScanner) {
        this(storageScanner, Clock.systemUTC());
    }

    /**
     * Creates a cleaner with an injectable clock for testability.
     *
     * @param storageScanner scanner for listing and deleting blob files
     * @param clock          clock for determining file age
     */
    public BlobOrphanCleaner(BlobStorageScanner storageScanner, Clock clock) {
        if (storageScanner == null) {
            throw new IllegalArgumentException("storageScanner must not be null");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        this.storageScanner = storageScanner;
        this.clock = clock;
    }

    /**
     * Cleans orphaned blob files for the given table asynchronously.
     *
     * <p>Scans all blob column directories under the table location, compares
     * discovered files against the set of referenced URIs, and deletes any
     * unreferenced file older than the grace period.</p>
     *
     * @param tableName       fully qualified table name (for logging and result)
     * @param blobPrefix      blob storage prefix (e.g., {table_location}/blob/)
     * @param blobColumns     column names that have blob storage directories
     * @param referencedUris  set of blob URIs currently referenced by data files
     * @param gracePeriod     minimum age before an unreferenced blob is eligible for deletion
     * @return future completing with the cleanup result
     */
    public CompletableFuture<BlobCleanupResult> cleanOrphansAsync(
            String tableName,
            String blobPrefix,
            List<String> blobColumns,
            Set<String> referencedUris,
            Duration gracePeriod) {

        return CompletableFuture.supplyAsync(() ->
                cleanOrphans(tableName, blobPrefix, blobColumns, referencedUris, gracePeriod));
    }

    /**
     * Synchronous orphan cleanup. Scans blob directories, identifies orphans,
     * and deletes those past the grace period.
     */
    BlobCleanupResult cleanOrphans(
            String tableName,
            String blobPrefix,
            List<String> blobColumns,
            Set<String> referencedUris,
            Duration gracePeriod) {

        validateInputs(tableName, blobPrefix, referencedUris);
        if (blobColumns == null || blobColumns.isEmpty()) {
            LOG.debug("event=blob_orphan_skip table={} reason=no_blob_columns", tableName);
            return new BlobCleanupResult(tableName, 0, 0L);
        }

        Duration effectiveGrace = gracePeriod != null ? gracePeriod : DEFAULT_GRACE_PERIOD;
        Instant cutoff = clock.instant().minus(effectiveGrace);

        int totalRemoved = 0;
        long totalBytesReclaimed = 0L;

        for (String column : blobColumns) {
            long[] columnResult = cleanColumn(tableName, blobPrefix, column, referencedUris, cutoff);
            totalRemoved += (int) columnResult[0];
            totalBytesReclaimed += columnResult[1];
        }

        LOG.info("event=blob_orphan_cleanup_complete table={} files_removed={} bytes_reclaimed={}",
                tableName, totalRemoved, totalBytesReclaimed);

        return new BlobCleanupResult(tableName, totalRemoved, totalBytesReclaimed);
    }

    private long[] cleanColumn(String tableName, String blobPrefix, String column,
            Set<String> referencedUris, Instant cutoff) {
        int removed = 0;
        long bytesReclaimed = 0L;
        String columnPrefix = normalizePrefix(blobPrefix) + column + "/";
        try {
            List<BlobFileInfo> files = storageScanner.listBlobFiles(columnPrefix);
            for (BlobFileInfo file : files) {
                if (isOrphan(file, referencedUris, cutoff) && deleteOrphan(file, tableName)) {
                    removed++;
                    bytesReclaimed += file.size();
                }
            }
        } catch (IOException e) {
            LOG.warn("event=blob_orphan_scan_failed table={} column={} error={}",
                    tableName, column, e.getMessage(), e);
        }
        return new long[] {removed, bytesReclaimed};
    }

    private static void validateInputs(String tableName, String blobPrefix, Set<String> referencedUris) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName must not be null or blank");
        }
        if (blobPrefix == null || blobPrefix.isBlank()) {
            throw new IllegalArgumentException("blobPrefix must not be null or blank");
        }
        if (referencedUris == null) {
            throw new IllegalArgumentException("referencedUris must not be null");
        }
    }

    private boolean isOrphan(BlobFileInfo file, Set<String> referencedUris, Instant cutoff) {
        return !referencedUris.contains(file.uri()) && file.lastModified().isBefore(cutoff);
    }

    private boolean deleteOrphan(BlobFileInfo file, String tableName) {
        try {
            storageScanner.delete(file.uri());
            LOG.debug("event=blob_orphan_deleted table={} uri={} size={}",
                    tableName, file.uri(), file.size());
            return true;
        } catch (IOException e) {
            LOG.warn("event=blob_orphan_delete_failed table={} uri={} error={}",
                    tableName, file.uri(), e.getMessage(), e);
            return false;
        }
    }

    private static String normalizePrefix(String prefix) {
        return prefix.endsWith("/") ? prefix : prefix + "/";
    }
}
