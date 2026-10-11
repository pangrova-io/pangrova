package org.pangrova.connector.iceberg.schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.connector.iceberg.type.IcebergTypeMapper;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.PangrovaTypeMapping;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.MapType;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Extracts partition spec, sort order, and table property handling from IcebergMetadata.
 * Stateless helper — all methods are static.
 */
public final class IcebergSchemaBuilder {

    /**
     * Offset applied to a column's nested field-ID base to allocate the map
     * value field ID (the key field uses the base itself).
     */
    private static final int MAP_VALUE_FIELD_ID_OFFSET = 500;

    private IcebergSchemaBuilder() {}

    /**
     * Builds an Iceberg {@link PartitionSpec} from the {@code partition-by}
     * table property. Uses identity transforms for each partition column.
     * Returns {@link PartitionSpec#unpartitioned()} when no partition columns
     * are specified.
     */
    public static PartitionSpec buildPartitionSpec(Schema schema, Map<String, String> properties) {
        String partitionBy = properties.get("partition-by");
        if (partitionBy == null || partitionBy.isBlank()) {
            return PartitionSpec.unpartitioned();
        }
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (String col : partitionBy.split(",")) {
            builder.identity(col.trim());
        }
        return builder.build();
    }

    /**
     * Builds an Iceberg {@link SortOrder} from the {@code sorted-by} table property.
     * Format: {@code "col1:asc,col2:desc"} where direction defaults to ASC if omitted.
     * Returns {@link SortOrder#unsorted()} when no sort columns are specified.
     */
    public static SortOrder buildSortOrder(Schema schema, Map<String, String> properties) {
        String sortedBy = properties.get("sorted-by");
        if (sortedBy == null || sortedBy.isBlank()) {
            return SortOrder.unsorted();
        }
        SortOrder.Builder builder = SortOrder.builderFor(schema);
        for (String entry : sortedBy.split(",")) {
            String trimmed = entry.trim();
            String columnName;
            boolean ascending;
            if (trimmed.contains(":")) {
                String[] parts = trimmed.split(":", 2);
                columnName = parts[0].trim();
                ascending = !"desc".equalsIgnoreCase(parts[1].trim());
            } else {
                columnName = trimmed;
                ascending = true;
            }
            if (ascending) {
                builder.asc(columnName, NullOrder.NULLS_LAST);
            } else {
                builder.desc(columnName, NullOrder.NULLS_LAST);
            }
        }
        return builder.build();
    }

    /**
     * Extracts Iceberg-native table properties from DDL properties map,
     * filtering out Pangrova-specific keys (partition-by, primary-key, sorted-by, sort-method).
     */
    public static Map<String, String> extractIcebergTableProperties(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return Map.of();
        }
        Map<String, String> icebergProps = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if ("partition-by".equals(key) || "primary-key".equals(key)
                    || "sorted-by".equals(key) || "sort-method".equals(key)) {
                continue;
            }
            icebergProps.put(key, entry.getValue());
        }
        return icebergProps;
    }

    /**
     * Builds an Iceberg {@link Schema} from typed column metadata, mapping each
     * Pangrova column type (including complex types) to its Iceberg counterpart.
     */
    public static Schema buildIcebergSchema(
            List<ConnectorTableMetadata.Column> columns, Map<String, String> properties) {
        List<Types.NestedField> fields = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            ConnectorTableMetadata.Column col = columns.get(i);
            int fieldId = col.fieldId().orElse(i + 1);
            // Use fieldId * 1000 as the base for nested sub-field IDs to avoid
            // collisions when multiple complex-type columns exist (e.g. multiple FILE columns).
            // Each column gets a 1000-ID namespace: column fieldId=2 → base 2000, fieldId=3 → 3000, etc.
            int nestedBaseFieldId = fieldId * 1000;
            Type icebergType = resolveColumnType(col, properties, nestedBaseFieldId);
            fields.add(Types.NestedField.optional(fieldId, col.name(), icebergType));
        }
        return new Schema(fields);
    }

    private static Type resolveColumnType(ConnectorTableMetadata.Column col,
                                          Map<String, String> properties,
                                          int nestedBaseFieldId) {
        if (col.type() == PangrovaType.VECTOR) {
            return buildVectorType(col, properties, nestedBaseFieldId);
        }
        Type parameterized = resolveParameterizedType(col, nestedBaseFieldId);
        if (parameterized != null) {
            return parameterized;
        }
        if (col.type() == PangrovaType.ARRAY) {
            return buildArrayType(col, properties, nestedBaseFieldId);
        }
        return IcebergTypeMapper.toIcebergType(col.type());
    }

    private static Type buildVectorType(ConnectorTableMetadata.Column col,
                                        Map<String, String> properties,
                                        int nestedBaseFieldId) {
        VectorType vector = requireDeclaredVector(col, properties);
        return IcebergTypeMapper.toIcebergVectorType(vector, nestedBaseFieldId);
    }

    /** Preserves the declared vector dimension alongside the exact SDK coordinate type. */
    public static Map<String, String> declaredTableProperties(
            List<ConnectorTableMetadata.Column> columns, Map<String, String> properties) {
        Map<String, String> declared = new LinkedHashMap<>(properties);
        for (ConnectorTableMetadata.Column column : columns) {
            if (column.type() == PangrovaType.VECTOR) {
                VectorType vector = requireDeclaredVector(column, properties);
                declared.put(IcebergTypeMapper.vectorDimensionKey(column.name()), Integer.toString(vector.dimension()));
            }
        }
        return Map.copyOf(declared);
    }

    private static VectorType requireDeclaredVector(ConnectorTableMetadata.Column column,
                                                      Map<String, String> properties) {
        if (!(column.parameterizedType() instanceof VectorType vector)) {
            throw new IllegalArgumentException("VECTOR requires its concrete storage declaration: " + column.name());
        }
        String dimension = properties.get(IcebergTypeMapper.vectorDimensionKey(column.name()));
        if (dimension != null && !Integer.toString(vector.dimension()).equals(dimension)) {
            throw new IllegalArgumentException("Vector dimension property disagrees with column " + column.name());
        }
        return vector;
    }

    private static Type resolveParameterizedType(ConnectorTableMetadata.Column col,
                                                 int nestedBaseFieldId) {
        if (col.type() == PangrovaType.STRUCT && col.parameterizedType() instanceof RowType rowType) {
            return IcebergTypeMapper.toIcebergTypeFromParameterized(rowType, nestedBaseFieldId);
        } else if (col.type() == PangrovaType.MAP && col.parameterizedType() instanceof MapType mapType) {
            return buildMapType(mapType, nestedBaseFieldId);
        } else if (col.type() == PangrovaType.DECIMAL
                && col.parameterizedType() instanceof DecimalType decType) {
            // Preserve the declared precision/scale (e.g. DECIMAL(10,2)).
            // The enum-only path below erases them to the DECIMAL(38,18)
            // default, which breaks both writes (wrong-scale rejection) and
            // native reads (Velox rescale of values written at another scale).
            return Types.DecimalType.of(decType.precision(), decType.scale());
        }
        return null;
    }

    private static Type buildMapType(MapType mapType, int nestedBaseFieldId) {
        int keyFieldId = nestedBaseFieldId;
        int valFieldId = nestedBaseFieldId + MAP_VALUE_FIELD_ID_OFFSET;
        Type keyIceberg = IcebergTypeMapper.toIcebergType(
                PangrovaTypeMapping.toPangrovaType(mapType.keyType()));
        Type valIceberg = IcebergTypeMapper.toIcebergType(
                PangrovaTypeMapping.toPangrovaType(mapType.valueType()));
        return Types.MapType.ofOptional(keyFieldId, valFieldId, keyIceberg, valIceberg);
    }

    private static Type buildArrayType(ConnectorTableMetadata.Column col,
                                       Map<String, String> properties,
                                       int nestedBaseFieldId) {
        String elemKey = "pangrova.array.element." + col.name();
        String elemTypeStr = properties.getOrDefault(elemKey, "VARCHAR");
        Type elementIcebergType;
        if (col.parameterizedType() instanceof ArrayType arrType) {
            elementIcebergType = IcebergTypeMapper.toIcebergType(
                    PangrovaTypeMapping.toPangrovaType(arrType.elementType()));
        } else {
            elementIcebergType = IcebergTypeMapper.toIcebergType(
                    PangrovaType.fromSqlTypeName(elemTypeStr));
        }
        return Types.ListType.ofOptional(nestedBaseFieldId, elementIcebergType);
    }

    /**
     * Applies the given sort order to the table via a {@code replaceSortOrder}
     * commit; a no-op when the sort order is unsorted.
     */
    public static void applySortOrder(Table table, Schema schema, SortOrder sortOrder) {
        if (sortOrder.isUnsorted()) {
            return;
        }
        var replaceSortOrder = table.replaceSortOrder();
        for (SortField field : sortOrder.fields()) {
            String colName = schema.findColumnName(field.sourceId());
            if (colName == null) {
                continue;
            }
            if (field.direction() == SortDirection.ASC) {
                replaceSortOrder.asc(colName, field.nullOrder());
            } else {
                replaceSortOrder.desc(colName, field.nullOrder());
            }
        }
        replaceSortOrder.commit();
    }
}
