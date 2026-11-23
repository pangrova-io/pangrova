package org.pangrova.connector.paimon.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.connector.api.index.SnapshotAwareMetadata;
import org.pangrova.connector.api.index.SnapshotDataFile;
import org.pangrova.connector.api.exception.SnapshotExpiredException;
import org.pangrova.connector.api.index.SnapshotFileDelta;
import org.pangrova.connector.api.enums.SnapshotRelation;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SnapshotManager;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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

    static final long MAXIMUM_SIZE = 10_000;
    static final long EXPIRE_AFTER_ACCESS_MINUTES = 5;

    private final Table table;
    private final Cache<String, SnapshotFileDelta> diffCache;

    public PaimonSnapshotAwareMetadata(Table table) {
        this(table, buildDefaultCache());
    }

    PaimonSnapshotAwareMetadata(Table table, Cache<String, SnapshotFileDelta> diffCache) {
        this.table = table;
        this.diffCache = diffCache;
    }

    private static Cache<String, SnapshotFileDelta> buildDefaultCache() {
        return Caffeine.newBuilder()
                .maximumSize(MAXIMUM_SIZE)
                .expireAfterAccess(EXPIRE_AFTER_ACCESS_MINUTES, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public Optional<String> currentSnapshotId() {
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            return Optional.empty();
        }
        Long snapshotId = fileStoreTable.snapshotManager().latestSnapshotId();
        return snapshotId == null
                ? Optional.empty()
                : Optional.of(Long.toString(snapshotId));
    }

    @Override
    public Optional<CurrentSnapshotFiles> currentSnapshotFiles() {
        return currentSnapshotId().map(snapshot -> new CurrentSnapshotFiles(snapshot, listFilesAtSnapshot(snapshot)));
    }

    @Override
    public List<SnapshotDataFile> listFilesAtSnapshot(String snapshotId) {
        return collectDataFiles(snapshotId).values().stream()
                .sorted(Comparator.comparing(SnapshotDataFile::path))
                .toList();
    }

    @Override
    public SnapshotRelation relation(String leftSnapshotId, String rightSnapshotId) {
        validateSnapshotExists(leftSnapshotId);
        validateSnapshotExists(rightSnapshotId);
        long left = Long.parseLong(leftSnapshotId);
        long right = Long.parseLong(rightSnapshotId);
        if (left == right) {
            return SnapshotRelation.SAME;
        }
        return left < right
                ? SnapshotRelation.LEFT_ANCESTOR
                : SnapshotRelation.RIGHT_ANCESTOR;
    }

    @Override
    public SnapshotAdvance observeAdvance(String fromSnapshotId, String toSnapshotId) {
        SnapshotRelation lineage = relation(fromSnapshotId, toSnapshotId);
        if (lineage != SnapshotRelation.LEFT_ANCESTOR) return SnapshotAdvance.noAdvance(lineage);
        var from = collectDataFiles(fromSnapshotId);
        var target = collectDataFiles(toSnapshotId);
        var files = target.values().stream().sorted(Comparator.comparing(SnapshotDataFile::path)).toList();
        return new SnapshotAdvance(lineage, files, SnapshotFileDelta.between(from, target));
    }

    @Override
    public SnapshotFileDelta diffFiles(String fromSnapshotId, String toSnapshotId) {
        String cacheKey = fromSnapshotId + ":" + toSnapshotId;
        return diffCache.get(cacheKey, key -> computeDiff(fromSnapshotId, toSnapshotId));
    }

    /** Clears the diff cache. Call after a batch of diff queries is complete. */
    public void clearDiffCache() {
        diffCache.invalidateAll();
    }

    private SnapshotFileDelta computeDiff(String fromSnapshotId, String toSnapshotId) {
        return SnapshotFileDelta.between(collectDataFiles(fromSnapshotId), collectDataFiles(toSnapshotId));
    }

    private Map<String, SnapshotDataFile> collectDataFiles(String snapshotId) {
        if (snapshotId == null) {
            return Map.of();
        }
        validateSnapshotExists(snapshotId);
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            throw new IllegalStateException("Snapshot file listing requires a FileStoreTable");
        }
        try {
            Table snapshotView = retainedSnapshotView(fileStoreTable, snapshotId);
            ReadBuilder readBuilder = snapshotView.newReadBuilder();
            Map<String, SnapshotDataFile> files = new LinkedHashMap<>();
            for (Split split : readBuilder.newScan().plan().splits()) {
                collectFilesFromSplit(split, files);
            }
            return Map.copyOf(files);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to read complete Paimon snapshot " + snapshotId, e);
        }
    }

    private static void collectFilesFromSplit(
            Split split, Map<String, SnapshotDataFile> files) {
        if (split instanceof DataSplit dataSplit) {
            for (DataFileMeta fileMeta : dataSplit.dataFiles()) {
                String path = resolvePath(dataSplit, fileMeta);
                String format = fileMeta.fileFormat();
                if (format == null || format.isBlank()) {
                    format = detectFormat(path);
                }
                SnapshotDataFile previous = files.put(path, new SnapshotDataFile(
                        path, fileMeta.rowCount(), fileMeta.fileSize(),
                        format.toLowerCase(Locale.ROOT)));
                if (previous != null) {
                    throw new IllegalStateException(
                            "Duplicate data file in Paimon snapshot: " + path);
                }
            }
        }
    }

    private static String resolvePath(DataSplit split, DataFileMeta file) {
        return file.externalPath().orElseGet(() -> {
            String bucketPath = split.bucketPath();
            if (bucketPath == null || bucketPath.isBlank()) {
                throw new IllegalStateException(
                        "Paimon split has no bucket path for " + file.fileName());
            }
            return bucketPath.endsWith("/")
                    ? bucketPath + file.fileName()
                    : bucketPath + "/" + file.fileName();
        });
    }

    private static String detectFormat(String path) {
        int dot = path.lastIndexOf('.');
        return dot >= 0 && dot + 1 < path.length()
                ? path.substring(dot + 1) : "unknown";
    }

    private void validateSnapshotExists(String snapshotId) {
        if (!(table instanceof FileStoreTable fst)) {
            throw new IllegalStateException("Snapshot validation requires a FileStoreTable");
        }
        long snapId = Long.parseLong(snapshotId);
        SnapshotManager snapshotManager = fst.snapshotManager();
        if (!snapshotManager.snapshotExists(snapId) && retainedTag(fst, snapId) == null) {
            throw new SnapshotExpiredException(snapshotId,
                    "Paimon snapshot " + snapshotId + " no longer exists");
        }
    }

    private static Table retainedSnapshotView(FileStoreTable table, String snapshotId) {
        long id = Long.parseLong(snapshotId);
        if (table.snapshotManager().snapshotExists(id)) {
            return table.copy(Map.of("scan.snapshot-id", snapshotId));
        }
        String tag = retainedTag(table, id);
        if (tag == null) {
            throw new SnapshotExpiredException(snapshotId,
                    "Paimon snapshot " + snapshotId + " no longer exists");
        }
        return table.copy(Map.of("scan.tag-name", tag));
    }

    private static String retainedTag(FileStoreTable table, long snapshotId) {
        return table.tagManager().tags().entrySet().stream()
                .filter(entry -> entry.getKey().id() == snapshotId)
                .flatMap(entry -> entry.getValue().stream())
                .sorted().findFirst().orElse(null);
    }
}
