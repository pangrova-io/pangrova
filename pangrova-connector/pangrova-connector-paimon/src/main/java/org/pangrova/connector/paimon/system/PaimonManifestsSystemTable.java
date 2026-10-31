package org.pangrova.connector.paimon.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.metadata.ConnectorSystemTable;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Paimon manifest files via {@code table$manifests}.
 *
 * <p>Each row represents a manifest file with its name, size,
 * and counts of added and deleted files.</p>
 */
public final class PaimonManifestsSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonManifestsSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "file_name", "file_size", "num_added_files", "num_deleted_files");

    private final Table table;

    public PaimonManifestsSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return List.copyOf(COLUMNS);
    }

    @Override
    public List<TypedRow> rows() {
        if (!(table instanceof FileStoreTable fst)) {
            return List.of();
        }
        SnapshotManager snapshotManager = fst.snapshotManager();
        Snapshot latest = snapshotManager.latestSnapshot();
        if (latest == null) {
            return List.of();
        }
        List<TypedRow> rows = new ArrayList<>();
        try {
            List<ManifestFileMeta> manifests = fst.store()
                    .manifestListFactory().create()
                    .read(latest.deltaManifestList());
            for (ManifestFileMeta manifest : manifests) {
                TypedRow row = TypedRow.builder()
                        .putString("file_name", manifest.fileName())
                        .putLong("file_size", manifest.fileSize())
                        .putLong("num_added_files", manifest.numAddedFiles())
                        .putLong("num_deleted_files", manifest.numDeletedFiles())
                        .build();
                rows.add(row);
            }
        } catch (Exception e) {
            LOG.warn("event=manifests_system_table_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }
}
