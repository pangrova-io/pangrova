package org.pangrova.connector.iceberg.index;

import org.pangrova.spi.connector.index.SnapshotAwareMetadata;
import org.pangrova.spi.connector.index.SnapshotExpiredException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Iceberg implementation of {@link SnapshotAwareMetadata}.
 *
 * <p>Computes file-level diffs between two Iceberg snapshots by scanning
 * data manifests at each snapshot and computing set differences. When the
 * starting snapshot has been expired, throws {@link SnapshotExpiredException}.</p>
 */
public final class IcebergSnapshotAwareMetadata implements SnapshotAwareMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSnapshotAwareMetadata.class);

    private final Catalog catalog;
    private final String schemaName;
    private final String tableName;

    public IcebergSnapshotAwareMetadata(Catalog catalog, String schemaName, String tableName) {
        this.catalog = catalog;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public List<String> listAddedFilesBetween(String fromSnapshotId, String toSnapshotId) {
        Table table = loadTable();
        Set<String> fromFiles = collectDataFiles(table, fromSnapshotId);
        Set<String> toFiles = collectDataFiles(table, toSnapshotId);
        List<String> added = new ArrayList<>();
        for (String file : toFiles) {
            if (!fromFiles.contains(file)) {
                added.add(file);
            }
        }
        return added;
    }

    @Override
    public List<String> listRemovedFilesBetween(String fromSnapshotId, String toSnapshotId) {
        Table table = loadTable();
        Set<String> fromFiles = collectDataFiles(table, fromSnapshotId);
        Set<String> toFiles = collectDataFiles(table, toSnapshotId);
        List<String> removed = new ArrayList<>();
        for (String file : fromFiles) {
            if (!toFiles.contains(file)) {
                removed.add(file);
            }
        }
        return removed;
    }

    private Set<String> collectDataFiles(Table table, String snapshotId) {
        if (snapshotId == null) {
            return Set.of();
        }
        validateSnapshotExists(table, snapshotId);
        long snapId = Long.parseLong(snapshotId);
        Snapshot snapshot = table.snapshot(snapId);
        Set<String> files = new HashSet<>();
        for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
            try (CloseableIterable<DataFile> reader =
                         ManifestFiles.read(manifest, table.io())) {
                for (DataFile dataFile : reader) {
                    files.add(dataFile.path().toString());
                }
            } catch (Exception e) {
                LOG.warn("action=manifest_read_failed snapshotId={} manifest={} error={}",
                        snapshotId, manifest.path(), e.getMessage());
            }
        }
        return files;
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

    private Table loadTable() {
        return catalog.loadTable(TableIdentifier.of(schemaName, tableName));
    }
}
