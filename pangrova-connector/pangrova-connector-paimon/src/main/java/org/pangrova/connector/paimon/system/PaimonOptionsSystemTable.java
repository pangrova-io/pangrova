package org.pangrova.connector.paimon.system;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.spi.metadata.ConnectorSystemTable;
import org.apache.paimon.table.Table;

/**
 * Exposes Paimon table options (properties) via {@code table$options}.
 *
 * <p>Each row represents a key-value pair from the table's configuration.</p>
 */
public final class PaimonOptionsSystemTable implements ConnectorSystemTable {

    private static final List<String> COLUMNS = List.of("key", "value");

    private final Table table;

    public PaimonOptionsSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return List.copyOf(COLUMNS);
    }

    @Override
    public List<TypedRow> rows() {
        Map<String, String> options = table.options();
        if (options == null || options.isEmpty()) {
            return List.of();
        }
        List<TypedRow> rows = new ArrayList<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            TypedRow row = TypedRow.builder()
                    .putString("key", entry.getKey())
                    .putString("value", entry.getValue())
                    .build();
            rows.add(row);
        }
        return List.copyOf(rows);
    }
}
