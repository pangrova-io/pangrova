package org.pangrova.connector.iceberg.index;

import org.pangrova.connector.api.index.SnapshotAwareMetadata;
import org.pangrova.connector.api.index.SnapshotDataFile;
import org.pangrova.connector.api.exception.SnapshotExpiredException;
import org.pangrova.connector.api.index.SnapshotFileDelta;
import org.pangrova.connector.api.enums.SnapshotRelation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Iceberg implementation of {@link SnapshotAwareMetadata}.
 *
 * <p>Computes file-level diffs between two Iceberg snapshots by scanning
 * data manifests at each snapshot and computing set differences. When the
 * starting snapshot has been expired, throws {@link SnapshotExpiredException}.</p>
 */
public final class IcebergSnapshotAwareMetadata implements SnapshotAwareMetadata {

    private final Catalog catalog;
    private final String schemaName;
    private final String tableName;

    public IcebergSnapshotAwareMetadata(Catalog catalog, String schemaName, String tableName) {
        this.catalog = catalog;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public Optional<String> currentSnapshotId() {
        Snapshot snapshot = loadTable().currentSnapshot();
        return snapshot == null
                ? Optional.empty()
                : Optional.of(Long.toString(snapshot.snapshotId()));
    }

    @Override
    public List<SnapshotDataFile> listFilesAtSnapshot(String snapshotId) {
        Table table = loadTable();
        return collectDataFiles(table, snapshotId).values().stream()
                .sorted(Comparator.comparing(SnapshotDataFile::path))
                .toList();
    }

    @Override
    public SnapshotRelation relation(String leftSnapshotId, String rightSnapshotId) {
        Table table = loadTable();
        validateSnapshotExists(table, leftSnapshotId);
        validateSnapshotExists(table, rightSnapshotId);
        if (leftSnapshotId.equals(rightSnapshotId)) {
            return SnapshotRelation.SAME;
        }
        long left = Long.parseLong(leftSnapshotId);
        long right = Long.parseLong(rightSnapshotId);
        if (isAncestor(table, left, right)) {
            return SnapshotRelation.LEFT_ANCESTOR;
        }
        if (isAncestor(table, right, left)) {
            return SnapshotRelation.RIGHT_ANCESTOR;
        }
        return SnapshotRelation.DIVERGED;
    }

    @Override
    public SnapshotFileDelta diffFiles(String fromSnapshotId, String toSnapshotId) {
        Table table = loadTable();
        Map<String, SnapshotDataFile> fromFiles = collectDataFiles(table, fromSnapshotId);
        Map<String, SnapshotDataFile> toFiles = collectDataFiles(table, toSnapshotId);
        List<SnapshotDataFile> added = new ArrayList<>();
        for (SnapshotDataFile file : toFiles.values()) {
            if (!fromFiles.containsKey(file.path())) {
                added.add(file);
            }
        }
        List<String> removed = new ArrayList<>();
        for (String path : fromFiles.keySet()) {
            if (!toFiles.containsKey(path)) {
                removed.add(path);
            }
        }
        added.sort(Comparator.comparing(SnapshotDataFile::path));
        removed.sort(String::compareTo);
        return new SnapshotFileDelta(added, removed);
    }

    private Map<String, SnapshotDataFile> collectDataFiles(Table table, String snapshotId) {
        if (snapshotId == null) {
            return Map.of();
        }
        validateSnapshotExists(table, snapshotId);
        long snapId = Long.parseLong(snapshotId);
        Snapshot snapshot = table.snapshot(snapId);
        Map<String, SnapshotDataFile> files = new LinkedHashMap<>();
        for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
            try (CloseableIterable<DataFile> reader =
                         ManifestFiles.read(manifest, table.io())) {
                for (DataFile dataFile : reader) {
                    String path = dataFile.path().toString();
                    SnapshotDataFile previous = files.put(path, new SnapshotDataFile(
                            path, dataFile.recordCount(), dataFile.fileSizeInBytes(),
                            dataFile.format().name().toLowerCase(java.util.Locale.ROOT)));
                    if (previous != null) {
                        throw new IllegalStateException(
                                "Duplicate data file in snapshot " + snapshotId + ": " + path);
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to read complete snapshot " + snapshotId
                                + " manifest " + manifest.path(), e);
            }
        }
        return Map.copyOf(files);
    }

    private void validateSnapshotExists(Table table, String snapshotId) {
        if (snapshotId == null) {
            return;
        }
        long id = Long.parseLong(snapshotId);
        if (table.snapshot(id) == null) {
            throw new SnapshotExpiredException(snapshotId,
                    "Snapshot " + snapshotId + " no longer exists in table "
                            + schemaName + "." + tableName);
        }
    }

    private static boolean isAncestor(Table table, long ancestorId, long descendantId) {
        Snapshot current = table.snapshot(descendantId);
        while (current != null && current.parentId() != null) {
            if (current.parentId() == ancestorId) {
                return true;
            }
            current = table.snapshot(current.parentId());
        }
        return false;
    }

    private Table loadTable() {
        return catalog.loadTable(TableIdentifier.of(schemaName, tableName));
    }
}
