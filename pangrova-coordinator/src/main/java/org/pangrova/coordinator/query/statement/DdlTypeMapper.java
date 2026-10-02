package org.pangrova.coordinator.query.statement;

import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.type.BaseType;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.MapType;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.PangrovaTypeMapping;
import org.pangrova.spi.connector.data.type.RowField;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.primitive.BigIntType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimestampType;

import java.util.List;
import java.util.Map;

/** Maps DDL type declarations to connector types and structural properties. */
final class DdlTypeMapper {

    private static final RowType FILE_STRUCT_TYPE = new RowType(List.of(
            new RowField("uri", VarcharType.UNBOUNDED),
            new RowField("mime_type", VarcharType.UNBOUNDED),
            new RowField("size", BigIntType.INSTANCE),
            new RowField("checksum", VarcharType.UNBOUNDED),
            new RowField("created_at", TimestampType.WITHOUT_TIMEZONE)));

    private DdlTypeMapper() {}

    static void injectStructuralProperties(
            List<CreateTableStatement.ColumnDefinition> columns,
            Map<String, String> properties) {
        for (CreateTableStatement.ColumnDefinition column : columns) {
            DataTypeSpec type = column.type();
            if (type instanceof DataTypeSpec.VectorType vector) {
                properties.put("pangrova.vector.dimension." + column.name(),
                        String.valueOf(vector.dimension()));
            }
            if (type instanceof DataTypeSpec.ArrayType array) {
                properties.put("pangrova.array.element." + column.name(),
                        array.elementType().toCanonicalString());
            }
            if (type instanceof DataTypeSpec.FileType) {
                properties.put("pangrova.file.column." + column.name(), "true");
            }
        }
    }

    static PangrovaType map(DataTypeSpec spec) {
        if (spec instanceof DataTypeSpec.PrimitiveType primitive) {
            return mapBaseType(primitive.baseType());
        }
        if (spec instanceof DataTypeSpec.DecimalType) {
            return PangrovaType.DECIMAL;
        }
        if (spec instanceof DataTypeSpec.ArrayType) {
            return PangrovaType.ARRAY;
        }
        if (spec instanceof DataTypeSpec.MapType) {
            return PangrovaType.MAP;
        }
        if (spec instanceof DataTypeSpec.StructType) {
            return PangrovaType.STRUCT;
        }
        if (spec instanceof DataTypeSpec.VectorType) {
            return PangrovaType.VECTOR;
        }
        if (spec instanceof DataTypeSpec.FileType) {
            return PangrovaType.STRUCT;
        }
        throw new IllegalArgumentException("Unknown DataTypeSpec variant: " + spec.getClass());
    }

    static Type parameterizedType(DataTypeSpec spec) {
        if (spec instanceof DataTypeSpec.StructType struct) {
            List<RowField> fields = struct.fields().stream()
                    .map(field -> new RowField(
                            field.name(), parameterizedType(field.type())))
                    .toList();
            return new RowType(fields);
        }
        if (spec instanceof DataTypeSpec.ArrayType array) {
            return new ArrayType(parameterizedType(array.elementType()));
        }
        if (spec instanceof DataTypeSpec.MapType map) {
            return new MapType(
                    parameterizedType(map.keyType()), parameterizedType(map.valueType()));
        }
        if (spec instanceof DataTypeSpec.FileType) {
            return FILE_STRUCT_TYPE;
        }
        if (spec instanceof DataTypeSpec.DecimalType decimal) {
            return new org.pangrova.spi.connector.data.type.primitive.DecimalType(
                    decimal.precision(), decimal.scale());
        }
        if (spec instanceof DataTypeSpec.VectorType vector) {
            return new org.pangrova.spi.connector.data.type.VectorType(vector.dimension(), null);
        }
        return PangrovaTypeMapping.toType(map(spec));
    }

    private static PangrovaType mapBaseType(BaseType baseType) {
        return switch (baseType) {
            case BOOLEAN -> PangrovaType.BOOLEAN;
            case TINYINT -> PangrovaType.TINYINT;
            case SMALLINT -> PangrovaType.SMALLINT;
            case INTEGER -> PangrovaType.INTEGER;
            case BIGINT -> PangrovaType.BIGINT;
            case REAL, FLOAT -> PangrovaType.FLOAT;
            case DOUBLE -> PangrovaType.DOUBLE;
            case VARCHAR, CHAR, STRING -> PangrovaType.VARCHAR;
            case VARBINARY, BLOB -> PangrovaType.VARBINARY;
            case DATE -> PangrovaType.DATE;
            case TIME -> PangrovaType.TIME;
            case TIMESTAMP -> PangrovaType.TIMESTAMP;
            case TIMESTAMP_NS -> PangrovaType.TIMESTAMP_NS;
            case INTERVAL -> PangrovaType.BIGINT;
            case UUID -> PangrovaType.UUID;
            case VARIANT -> PangrovaType.VARIANT;
            case GEOMETRY -> PangrovaType.GEOMETRY;
            case GEOGRAPHY -> PangrovaType.GEOGRAPHY;
        };
    }
}
