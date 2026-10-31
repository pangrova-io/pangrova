package org.pangrova.connector.paimon.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.metadata.ConnectorSystemTable;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Paimon consumer progress via {@code table$consumers}.
 *
 * <p>Each row represents a consumer with its ID and the next snapshot
 * it should consume from.</p>
 */
public final class PaimonConsumersSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonConsumersSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "consumer_id", "next_snapshot_id");

    private final Table table;

    public PaimonConsumersSystemTable(Table table) {
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
        List<TypedRow> rows = new ArrayList<>();
        try {
            ConsumerManager consumerManager = new ConsumerManager(
                    fst.fileIO(), fst.location(), fst.snapshotManager().branch());
            consumerManager.consumers().forEach((consumerId, nextSnapshotId) -> {
                TypedRow row = TypedRow.builder()
                        .putString("consumer_id", consumerId)
                        .putLong("next_snapshot_id", nextSnapshotId)
                        .build();
                rows.add(row);
            });
        } catch (Exception e) {
            LOG.warn("event=consumers_system_table_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }
}
