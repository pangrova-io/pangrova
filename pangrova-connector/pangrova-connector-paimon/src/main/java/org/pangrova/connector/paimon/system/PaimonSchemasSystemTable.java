package org.pangrova.connector.paimon.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.ConnectorSystemTable;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Paimon schema evolution history via {@code table$schemas}.
 *
 * <p>Each row represents a schema version with its ID, field definitions,
 * partition keys, and primary keys.</p>
 */
public final class PaimonSchemasSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSchemasSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "schema_id", "fields", "partition_keys", "primary_keys");

    private final Table table;

    public PaimonSchemasSystemTable(Table table) {
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
        SchemaManager schemaManager = fst.schemaManager();
        List<TypedRow> rows = new ArrayList<>();
        try {
            for (TableSchema schema : schemaManager.listAll()) {
                TypedRow row = TypedRow.builder()
                        .putLong("schema_id", schema.id())
                        .putString("fields", schema.fields().toString())
                        .putString("partition_keys", schema.partitionKeys().toString())
                        .putString("primary_keys", schema.primaryKeys().toString())
                        .build();
                rows.add(row);
            }
        } catch (Exception e) {
            LOG.warn("event=schemas_system_table_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }
}
