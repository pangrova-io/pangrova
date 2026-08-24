package org.pangrova.connector.paimon.system;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.TagManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Paimon tags via {@code table$tags}.
 *
 * <p>Each row represents a named tag with its associated snapshot ID
 * and creation time.</p>
 */
public final class PaimonTagsSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonTagsSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "tag_name", "snapshot_id", "create_time");

    private final Table table;

    public PaimonTagsSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }

    @Override
    public List<TypedRow> rows() {
        if (!(table instanceof FileStoreTable fst)) {
            return List.of();
        }
        TagManager tagManager = fst.tagManager();
        List<TypedRow> rows = new ArrayList<>();
        try {
            SortedMap<Snapshot, List<String>> tagMap = tagManager.tags();
            for (Map.Entry<Snapshot, List<String>> entry : tagMap.entrySet()) {
                Snapshot snapshot = entry.getKey();
                for (String tagName : entry.getValue()) {
                    TypedRow row = TypedRow.builder()
                            .putString("tag_name", tagName)
                            .putLong("snapshot_id", snapshot.id())
                            .putLong("create_time", snapshot.timeMillis())
                            .build();
                    rows.add(row);
                }
            }
        } catch (Exception e) {
            LOG.warn("event=tags_system_table_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }
}
