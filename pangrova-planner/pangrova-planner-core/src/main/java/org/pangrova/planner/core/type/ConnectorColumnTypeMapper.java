package org.pangrova.planner.core.type;

import org.pangrova.types.PangrovaType;
import org.pangrova.types.mapping.PangrovaTypeMapping;
import org.pangrova.types.ArrayType;
import org.pangrova.types.MapType;
import org.pangrova.types.NullOnlyType;
import org.pangrova.types.RowField;
import org.pangrova.types.RowType;
import org.pangrova.types.Type;
import org.pangrova.types.VectorType;
import org.pangrova.types.primitive.DecimalType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.protocol.type.VectorLogicalType;

import java.util.ArrayList;
import java.util.List;

/** Single planner-owned mapping of connector columns to relational storage types. */
public final class ConnectorColumnTypeMapper {
    private ConnectorColumnTypeMapper() {}

    /** Preserves ordered column names, nested structure and storage precision. */
    public static RelDataType rowType(
            List<ConnectorTableMetadata.Column> typedColumns, RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ConnectorTableMetadata.Column col : typedColumns) {
            builder.add(col.name(), mapToSqlType(col.type(), col.parameterizedType(), typeFactory));
        }
        return builder.build();
    }

    private static RelDataType mapToSqlType(PangrovaType type,
                                            Type parameterizedType,
                                            RelDataTypeFactory factory) {
        requireConcreteDeclaration(type, parameterizedType);
        if (type == PangrovaType.UNKNOWN) {
            return factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.VARCHAR), true);
        }
        if (type == PangrovaType.VECTOR) {
            var vector = (VectorType) parameterizedType;
            var logical = VectorLogicalType.create(vector.dimension(),
                    vector.elementType() == VectorType.ElementType.FLOAT
                            ? VectorLogicalType.FLOAT32 : VectorLogicalType.FLOAT64, true, false);
            return StorageVectorRelDataType.create(logical, factory);
        }
        if (type == PangrovaType.ARRAY) {
            RelDataType elementType = resolveArrayElementType(parameterizedType, factory);
            return factory.createTypeWithNullability(
                    factory.createArrayType(elementType, -1), true);
        }
        if (type == PangrovaType.MAP && parameterizedType instanceof MapType mapType) {
            RelDataType keyType = mapToSqlType(
                    PangrovaTypeMapping.toPangrovaType(mapType.keyType()),
                    mapType.keyType(), factory);
            RelDataType valueType = mapToSqlType(
                    PangrovaTypeMapping.toPangrovaType(mapType.valueType()),
                    mapType.valueType(), factory);
            return factory.createTypeWithNullability(
                    factory.createMapType(keyType, valueType), true);
        }
        if (type == PangrovaType.STRUCT && parameterizedType instanceof RowType rowType) {
            return mapStruct(rowType, factory);
        }
        if (type == PangrovaType.DECIMAL && parameterizedType instanceof DecimalType decType) {
            // Preserve declared precision/scale. A bare createSqlType(DECIMAL)
            // yields Calcite's default (scale 0), so the plan would disagree
            // with the stored file scale and native reads would attempt an
            // unsupported rescale ("Parquet does not support scale narrowing").
            return factory.createTypeWithNullability(
                    factory.createSqlType(SqlTypeName.DECIMAL,
                            decType.precision(), decType.scale()),
                    true);
        }
        if (type == PangrovaType.TIMESTAMP_NS) {
            return factory.createTypeWithNullability(
                    factory.createSqlType(SqlTypeName.TIMESTAMP, 9), true);
        }
        return factory.createTypeWithNullability(factory.createSqlType(primitiveSqlType(type)), true);
    }

    private static RelDataType mapStruct(RowType rowType, RelDataTypeFactory factory) {
        // Named ROW fields support subfield lookup without expanding SELECT *
        // into physical columns absent from the underlying storage schema.
        List<String> names = new ArrayList<>(rowType.fields().size());
        List<RelDataType> types = new ArrayList<>(rowType.fields().size());
        for (RowField field : rowType.fields()) {
            names.add(field.name());
            types.add(mapToSqlType(PangrovaTypeMapping.toPangrovaType(field.type()), field.type(), factory));
        }
        return factory.createTypeWithNullability(
                factory.createStructType(StructKind.PEEK_FIELDS_NO_EXPAND, types, names), true);
    }

    private static SqlTypeName primitiveSqlType(PangrovaType type) {
        return switch (type) {
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
            case UUID -> SqlTypeName.UUID;
            case STRUCT -> throw new IllegalArgumentException(
                    "STRUCT type without field metadata (RowType); cannot map to SQL type");
            case MAP -> SqlTypeName.MAP;
            case INTERVAL_YEAR_MONTH -> SqlTypeName.INTERVAL_YEAR_MONTH;
            case INTERVAL_DAY_TIME -> SqlTypeName.INTERVAL_DAY_SECOND;
            case UNKNOWN -> throw new IllegalArgumentException("UNKNOWN has no concrete SQL storage type");
            default -> throw new IllegalArgumentException("Unmapped PangrovaType: " + type);
        };
    }

    private static RelDataType resolveArrayElementType(
            Type parameterizedType,
            RelDataTypeFactory factory) {
        if (parameterizedType instanceof ArrayType arrayType) {
            Type elemType = arrayType.elementType();
            PangrovaType elemPangrovaType = PangrovaTypeMapping.toPangrovaType(elemType);
            return mapToSqlType(elemPangrovaType, elemType, factory);
        }
        throw new IllegalArgumentException("ARRAY or VECTOR requires its complete element declaration");
    }

    private static void requireConcreteDeclaration(PangrovaType type, Type parameters) {
        boolean complete = switch (type) {
            case UNKNOWN -> parameters instanceof NullOnlyType;
            case ARRAY -> parameters instanceof ArrayType;
            case VECTOR -> parameters instanceof VectorType;
            case MAP -> parameters instanceof MapType;
            case STRUCT -> parameters instanceof RowType;
            case DECIMAL -> parameters instanceof DecimalType;
            default -> true;
        };
        if (!complete) {
            throw new IllegalArgumentException(type + " requires a concrete matching storage type declaration");
        }
    }
}
