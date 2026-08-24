package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a table with typed columns resolved from ConnectorTableMetadata.
 * Provides schema metadata (column names and types) to Calcite's planning
 * infrastructure via {@link #getRowType}.
 *
 * <p><b>P2-2 fix:</b> Previously implemented {@code ScannableTable} with a
 * {@code scan()} method that read ALL data from the connector into coordinator
 * memory — an OOM risk on production-sized tables. The coordinator's role is
 * PLANNING only; query execution runs on workers via Velox/native. The
 * {@code ScannableTable} interface and all data-reading code have been removed
 * to enforce this boundary and prevent accidental coordinator-side table scans.</p>
 */
final class MetadataResolvedTable extends AbstractTable {

    private final List<ConnectorTableMetadata.Column> typedColumns;
    private final CatalogHandle catalogHandle;
    private final String catalog;
    private final String schema;
    private final String table;

    MetadataResolvedTable(List<ConnectorTableMetadata.Column> typedColumns,
                          CatalogHandle catalogHandle,
                          String catalog, String schema, String table) {
        this.typedColumns = typedColumns;
        this.catalogHandle = catalogHandle;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ConnectorTableMetadata.Column col : typedColumns) {
            builder.add(col.name(), mapToSqlType(col.type(), col.parameterizedType(), typeFactory));
        }
        return builder.build();
    }

    private static RelDataType mapToSqlType(PangrovaType type, RelDataTypeFactory factory) {
        return mapToSqlType(type, null, factory);
    }

    private static RelDataType mapToSqlType(PangrovaType type,
                                            org.pangrova.spi.connector.data.type.Type parameterizedType,
                                            RelDataTypeFactory factory) {
        if (type == PangrovaType.ARRAY || type == PangrovaType.VECTOR) {
            RelDataType elementType = resolveArrayElementType(parameterizedType, factory);
            return factory.createTypeWithNullability(
                    factory.createArrayType(elementType, -1), true);
        }
        if (type == PangrovaType.MAP && parameterizedType instanceof org.pangrova.spi.connector.data.type.MapType mapType) {
            RelDataType keyType = mapToSqlType(
                    org.pangrova.spi.connector.data.type.PangrovaTypeMapping.toPangrovaType(mapType.keyType()),
                    mapType.keyType(), factory);
            RelDataType valueType = mapToSqlType(
                    org.pangrova.spi.connector.data.type.PangrovaTypeMapping.toPangrovaType(mapType.valueType()),
                    mapType.valueType(), factory);
            return factory.createTypeWithNullability(
                    factory.createMapType(keyType, valueType), true);
        }
        if (type == PangrovaType.STRUCT && parameterizedType instanceof RowType rowType) {
            // Build a Calcite struct (SqlTypeName.ROW) preserving field names and
            // types so SqlValidator.fullyQualify can resolve struct field access
            // (e.g. SELECT info.name FROM ...). SqlTypeName.STRUCTURED maps to a
            // user-defined type with a null fieldList in Calcite, which breaks
            // both validation and downstream Substrait conversion.
            //
            // StructKind.PEEK_FIELDS_NO_EXPAND lets Calcite resolve "info.name"
            // without a table alias (DelegatingScope.fullyQualify only allows
            // unqualified subfield access for PEEK_FIELDS* kinds; otherwise it
            // throws "Table 'info' not found"). NO_EXPAND keeps SELECT *
            // returning the struct as a single column rather than expanding
            // sub-columns -- otherwise Calcite emits one Project field per
            // sub-field and the downstream Substrait/native plan tries to read
            // beyond the actual schema width.
            List<String> fieldNames = new ArrayList<>(rowType.fields().size());
            List<RelDataType> fieldTypes = new ArrayList<>(rowType.fields().size());
            for (org.pangrova.spi.connector.data.type.RowField field : rowType.fields()) {
                fieldNames.add(field.name());
                PangrovaType fieldPangrovaType =
                        org.pangrova.spi.connector.data.type.PangrovaTypeMapping.toPangrovaType(field.type());
                fieldTypes.add(mapToSqlType(fieldPangrovaType, field.type(), factory));
            }
            return factory.createTypeWithNullability(
                    factory.createStructType(
                            org.apache.calcite.rel.type.StructKind.PEEK_FIELDS_NO_EXPAND,
                            fieldTypes,
                            fieldNames),
                    true);
        }
        if (type == PangrovaType.DECIMAL && parameterizedType
                instanceof org.pangrova.spi.connector.data.type.primitive.DecimalType decType) {
            // Preserve declared precision/scale. A bare createSqlType(DECIMAL)
            // yields Calcite's default (scale 0), so the plan would disagree
            // with the stored file scale and native reads would attempt an
            // unsupported rescale ("Parquet does not support scale narrowing").
            return factory.createTypeWithNullability(
                    factory.createSqlType(SqlTypeName.DECIMAL,
                            decType.precision(), decType.scale()),
                    true);
        }
        SqlTypeName sqlType = switch (type) {
            case BOOLEAN -> SqlTypeName.BOOLEAN;
            case TINYINT -> SqlTypeName.TINYINT;
            case SMALLINT -> SqlTypeName.SMALLINT;
            case INTEGER -> SqlTypeName.INTEGER;
            case BIGINT -> SqlTypeName.BIGINT;
            case FLOAT -> SqlTypeName.FLOAT;
            case DOUBLE -> SqlTypeName.DOUBLE;
            case DECIMAL -> SqlTypeName.DECIMAL;
            case DATE -> SqlTypeName.DATE;
            case TIME -> SqlTypeName.TIME;
            case TIMESTAMP -> SqlTypeName.TIMESTAMP;
            case TIMESTAMP_TZ -> SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            case VARCHAR -> SqlTypeName.VARCHAR;
            case VARBINARY, FIXED, GEOMETRY, GEOGRAPHY -> SqlTypeName.VARBINARY;
            case VARIANT -> SqlTypeName.VARCHAR;
            case TIMESTAMP_NS -> SqlTypeName.TIMESTAMP;
            case UUID -> SqlTypeName.CHAR;
            case STRUCT -> throw new IllegalArgumentException(
                    "STRUCT type without field metadata (RowType); cannot map to SQL type");
            case MAP -> SqlTypeName.MAP;
            case INTERVAL_YEAR_MONTH -> SqlTypeName.INTERVAL_YEAR_MONTH;
            case INTERVAL_DAY_TIME -> SqlTypeName.INTERVAL_DAY_SECOND;
            case UNKNOWN -> SqlTypeName.VARCHAR;
            default -> throw new IllegalArgumentException("Unmapped PangrovaType: " + type);
        };
        return factory.createTypeWithNullability(factory.createSqlType(sqlType), true);
    }

    private static RelDataType resolveArrayElementType(
            org.pangrova.spi.connector.data.type.Type parameterizedType,
            RelDataTypeFactory factory) {
        if (parameterizedType instanceof org.pangrova.spi.connector.data.type.ArrayType arrayType) {
            org.pangrova.spi.connector.data.type.Type elemType = arrayType.elementType();
            PangrovaType elemPangrovaType = org.pangrova.spi.connector.data.type.PangrovaTypeMapping.toPangrovaType(elemType);
            return mapToSqlType(elemPangrovaType, elemType, factory);
        }
        // Fallback for VECTOR or when parameterized type is unavailable
        return factory.createSqlType(SqlTypeName.DOUBLE);
    }
}
