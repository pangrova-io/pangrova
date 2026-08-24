package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.metadata.table.ColumnMapping;
import org.pangrova.spi.connector.metadata.table.TypePromotion;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts partition values and schema evolution metadata from Paimon splits.
 */
final class PaimonPartitionExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPartitionExtractor.class);

    private PaimonPartitionExtractor() {}

    // ── Partition value extraction (R12.3, R12.4) ──

    /**
     * Extracts partition column values from a Paimon SDK Split.
     *
     * @return partition column name → string value map; empty if not partitioned
     */
    static Map<String, String> extractPartitionValues(Split split, Table table) {
        List<String> partitionKeys = table.partitionKeys();
        if (partitionKeys.isEmpty() || !(split instanceof DataSplit dataSplit)) {
            return Map.of();
        }

        BinaryRow partitionRow = dataSplit.partition();
        if (partitionRow == null || partitionRow.getFieldCount() == 0) {
            return Map.of();
        }

        return buildPartitionMap(partitionKeys, partitionRow, table.rowType());
    }

    private static Map<String, String> buildPartitionMap(
            List<String> partitionKeys, BinaryRow partitionRow, RowType rowType) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            String partitionKey = partitionKeys.get(i);
            if (i < partitionRow.getFieldCount() && !partitionRow.isNullAt(i)) {
                String value = extractFieldValue(partitionRow, i, rowType, partitionKey);
                if (value != null) {
                    values.put(partitionKey, value);
                }
            }
        }
        return Map.copyOf(values);
    }

    private static String extractFieldValue(
            BinaryRow row, int index, RowType rowType, String fieldName) {
        DataField field = findField(rowType, fieldName);
        if (field == null) {
            return row.getString(index).toString();
        }
        return switch (field.type().getTypeRoot()) {
            case INTEGER -> String.valueOf(row.getInt(index));
            case BIGINT -> String.valueOf(row.getLong(index));
            case SMALLINT -> String.valueOf(row.getShort(index));
            case TINYINT -> String.valueOf(row.getByte(index));
            case FLOAT -> String.valueOf(row.getFloat(index));
            case DOUBLE -> String.valueOf(row.getDouble(index));
            case BOOLEAN -> String.valueOf(row.getBoolean(index));
            case DATE -> String.valueOf(row.getInt(index));
            default -> row.getString(index).toString();
        };
    }

    private static DataField findField(RowType rowType, String fieldName) {
        for (DataField field : rowType.getFields()) {
            if (field.name().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    // ── Schema evolution extraction (R13.1–R13.4) ──

    /**
     * Extracts schema evolution column mappings and type promotions for a split.
     */
    static SchemaEvolutionResult extractSchemaEvolution(Split split, Table table) {
        if (!(split instanceof DataSplit dataSplit)) {
            return SchemaEvolutionResult.EMPTY;
        }

        RowType currentRowType = table.rowType();
        List<DataField> currentFields = currentRowType.getFields();

        if (!hasSchemaEvolution(dataSplit, currentFields)) {
            return SchemaEvolutionResult.EMPTY;
        }

        List<ColumnMapping> columnMappings = buildColumnMappings(currentFields);
        List<TypePromotion> typePromotions = List.of();

        LOG.debug("event=paimon_schema_evolution_detected field_count={} mappings={}",
                currentFields.size(), columnMappings.size());

        return new SchemaEvolutionResult(columnMappings, typePromotions);
    }

    private static boolean hasSchemaEvolution(DataSplit dataSplit, List<DataField> currentFields) {
        List<DataFileMeta> dataFiles = dataSplit.dataFiles();
        if (currentFields.isEmpty() || dataFiles == null || dataFiles.isEmpty()) {
            return false;
        }
        long maxSchemaId = Long.MIN_VALUE;
        long minSchemaId = Long.MAX_VALUE;
        for (DataFileMeta file : dataFiles) {
            long sid = file.schemaId();
            if (sid > maxSchemaId) maxSchemaId = sid;
            if (sid < minSchemaId) minSchemaId = sid;
        }
        return minSchemaId != maxSchemaId;
    }

    private static List<ColumnMapping> buildColumnMappings(List<DataField> currentFields) {
        List<ColumnMapping> mappings = new ArrayList<>(currentFields.size());
        for (DataField field : currentFields) {
            mappings.add(new ColumnMapping(
                    field.id(), field.id(),
                    field.name(), field.name()));
        }
        return List.copyOf(mappings);
    }

    /**
     * Immutable result of schema evolution extraction for a single split.
     */
    record SchemaEvolutionResult(
            List<ColumnMapping> columnMappings,
            List<TypePromotion> typePromotions) {

        static final SchemaEvolutionResult EMPTY =
                new SchemaEvolutionResult(List.of(), List.of());
    }
}
