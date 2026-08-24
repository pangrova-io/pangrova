package org.pangrova.connector.paimon.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.spi.connector.index.SnapshotAwareMetadata;
import org.pangrova.spi.connector.index.SnapshotExpiredException;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Paimon implementation of {@link SnapshotAwareMetadata}.
 *
 * <p>Computes file-level diffs between two Paimon snapshots by scanning
 * data files at each snapshot and computing set differences. When the
 * starting snapshot has been expired, throws {@link SnapshotExpiredException}.</p>
 *
 * <p>The diff cache is bounded by {@code maximumSize(10_000)} and entries expire
 * after {@code 5 minutes} of no access, preventing unbounded memory growth.</p>
 */
public final class PaimonSnapshotAwareMetadata implements SnapshotAwareMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSnapshotAwareMetadata.class);

    static final long MAXIMUM_SIZE = 10_000;
    static final long EXPIRE_AFTER_ACCESS_MINUTES = 5;

    private final Table table;
    private final Cache<String, SnapshotDiff> diffCache;

    public PaimonSnapshotAwareMetadata(Table table) {
        this(table, buildDefaultCache());
    }

    PaimonSnapshotAwareMetadata(Table table, Cache<String, SnapshotDiff> diffCache) {
        this.table = table;
        this.diffCache = diffCache;
    }

    private static Cache<String, SnapshotDiff> buildDefaultCache() {
        return Caffeine.newBuilder()
                .maximumSize(MAXIMUM_SIZE)
                .expireAfterAccess(EXPIRE_AFTER_ACCESS_MINUTES, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public List<String> listAddedFilesBetween(String fromSnapshotId, String toSnapshotId) {
        return computeDiff(fromSnapshotId, toSnapshotId).added();
    }

    @Override
    public List<String> listRemovedFilesBetween(String fromSnapshotId, String toSnapshotId) {
        return computeDiff(fromSnapshotId, toSnapshotId).removed();
    }

    /** Clears the diff cache. Call after a batch of diff queries is complete. */
    public void clearDiffCache() {
        diffCache.invalidateAll();
    }

    private SnapshotDiff computeDiff(String fromSnapshotId, String toSnapshotId) {
        String cacheKey = fromSnapshotId + ":" + toSnapshotId;
        return diffCache.get(cacheKey, k -> {
            Set<String> fromFiles = collectDataFiles(fromSnapshotId);
            Set<String> toFiles = collectDataFiles(toSnapshotId);

            List<String> added = new ArrayList<>();
            for (String file : toFiles) {
                if (!fromFiles.contains(file)) {
                    added.add(file);
                }
            }

            List<String> removed = new ArrayList<>();
            for (String file : fromFiles) {
                if (!toFiles.contains(file)) {
                    removed.add(file);
                }
            }

            return new SnapshotDiff(List.copyOf(added), List.copyOf(removed));
        });
    }

    record SnapshotDiff(List<String> added, List<String> removed) {}

    private Set<String> collectDataFiles(String snapshotId) {
        if (snapshotId == null) {
            return Set.of();
        }
        validateSnapshotExists(snapshotId);
        Set<String> files = new HashSet<>();
        try {
            Table snapshotView = ((FileStoreTable) table).copy(
                    Map.of("scan.snapshot-id", snapshotId));
            ReadBuilder readBuilder = snapshotView.newReadBuilder();
            for (Split split : readBuilder.newScan().plan().splits()) {
                collectFilesFromSplit(split, files);
            }
        } catch (SnapshotExpiredException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("action=collect_data_files_failed snapshotId={} error={}",
                    snapshotId, e.getMessage());
        }
        return files;
    }

    private static void collectFilesFromSplit(Split split, Set<String> files) {
        if (split instanceof DataSplit dataSplit) {
            for (DataFileMeta fileMeta : dataSplit.dataFiles()) {
                files.add(fileMeta.fileName());
            }
        }
    }

    private void validateSnapshotExists(String snapshotId) {
        if (!(table instanceof FileStoreTable fst)) {
            return;
        }
        long snapId = Long.parseLong(snapshotId);
        SnapshotManager snapshotManager = fst.snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapId);
        if (snapshot == null) {
            throw new SnapshotExpiredException(snapshotId,
                    "Paimon snapshot " + snapshotId + " no longer exists");
        }
    }
}
