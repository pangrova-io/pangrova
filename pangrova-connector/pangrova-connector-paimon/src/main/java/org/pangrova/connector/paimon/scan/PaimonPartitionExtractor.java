package org.pangrova.connector.paimon.scan;

import org.pangrova.spi.annotation.Internal;

import org.pangrova.spi.connector.metadata.table.ColumnMapping;
import org.pangrova.spi.connector.metadata.table.TypePromotion;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts partition values and schema evolution metadata from Paimon splits.
 */
@Internal
public final class PaimonPartitionExtractor {

    private PaimonPartitionExtractor() {}

    // ── Partition value extraction (R12.3, R12.4) ──

    /**
     * Extracts partition column values from a Paimon SDK Split.
     *
     * @return partition column name → string value map; empty if not partitioned
     */
    public static Map<String, String> extractPartitionValues(Split split, Table table) {
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
    public static SchemaEvolutionResult extractSchemaEvolution(DataFileMeta file, Table target) {
        if (!(target instanceof FileStoreTable table) || !table.schemaManager().schemaExists(file.schemaId())) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Paimon physical file has no exact schema-id=" + file.schemaId());
        }
        var source = table.schemaManager().schema(file.schemaId());
        Map<Integer, DataField> sourceFields = new LinkedHashMap<>();
        source.fields().forEach(field -> sourceFields.put(field.id(), field));
        List<ColumnMapping> mappings = new ArrayList<>();
        List<TypePromotion> promotions = new ArrayList<>();
        for (var field : target.rowType().getFields()) {
            var physical = sourceFields.get(field.id());
            requireRepresentedMissingColumn(physical, field, target.options());
            mappings.add(new ColumnMapping(physical == null ? -1 : physical.id(), field.id(),
                    physical == null ? field.name() : physical.name(), field.name()));
            addTypePromotion(physical, field, promotions);
        }
        return new SchemaEvolutionResult(mappings, promotions);
    }

    private static void requireRepresentedMissingColumn(DataField physical, DataField target,
            Map<String, String> options) {
        String policy = "fields." + target.name() + ".default-value";
        if (physical == null && options.containsKey(policy)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Paimon Native schema evolution does not represent SDK policy: " + policy);
        }
    }

    private static void addTypePromotion(DataField physical, DataField target, List<TypePromotion> promotions) {
        if (physical == null || physical.type().copy(true).equals(target.type().copy(true))) {
            return;
        }
        String sourceType = promotionType(physical.type());
        String targetType = promotionType(target.type());
        if (!sourceType.equals(targetType)) {
            promotions.add(new TypePromotion(target.id(), sourceType, targetType, target.name()));
        }
    }

    private static String promotionType(org.apache.paimon.types.DataType type) {
        return switch (type.getTypeRoot()) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE -> type.getTypeRoot().name();
            case CHAR, VARCHAR -> "varchar";
            case BINARY, VARBINARY -> "varbinary";
            case DECIMAL -> {
                var decimal = (org.apache.paimon.types.DecimalType) type;
                yield "decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
            }
            default -> throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Unsupported Paimon physical type evolution: " + type);
        };
    }

    /**
     * Immutable result of schema evolution extraction for a single split.
     */
    public record SchemaEvolutionResult(
            List<ColumnMapping> columnMappings,
            List<TypePromotion> typePromotions) {

        public SchemaEvolutionResult {
            columnMappings = List.copyOf(columnMappings);
            typePromotions = List.copyOf(typePromotions);
        }

        static final SchemaEvolutionResult EMPTY =
                new SchemaEvolutionResult(List.of(), List.of());
    }
}
