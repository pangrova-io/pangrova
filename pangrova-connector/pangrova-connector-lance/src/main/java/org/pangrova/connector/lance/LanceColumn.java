package org.pangrova.connector.lance;

import org.pangrova.types.ArrayType;
import org.pangrova.types.MapType;
import org.pangrova.types.PangrovaType;
import org.pangrova.types.mapping.PangrovaTypeMapping;
import org.pangrova.types.RowField;
import org.pangrova.types.RowType;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;
import org.pangrova.types.VectorType;
import org.pangrova.types.primitive.CharType;
import org.pangrova.types.primitive.DecimalType;
import org.pangrova.types.primitive.RealType;
import org.pangrova.types.primitive.VarcharType;
import org.pangrova.types.primitive.temporal.IntervalDayTimeType;
import org.pangrova.types.primitive.temporal.IntervalYearMonthType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import java.util.List;
import java.util.OptionalInt;

/** Durable ordered column identity, retaining all structural type parameters. */
public record LanceColumn(String name, PangrovaType type, TypeSignature signature) {
    public LanceColumn {
        if (name == null || name.isBlank() || type == null || signature == null) {
            throw new IllegalArgumentException("Lance column requires a name and exact type");
        }
    }

    static LanceColumn from(ConnectorTableMetadata.Column column) {
        Type parameterized = column.parameterizedType() == null
                ? PangrovaTypeMapping.toType(column.type()) : column.parameterizedType();
        if (parameterized == null) {
            throw new IllegalArgumentException("Lance column requires structural type parameters");
        }
        return new LanceColumn(column.name(), column.type(), parameterized.getTypeSignature());
    }

    ConnectorTableMetadata.Column metadata() {
        return new ConnectorTableMetadata.Column(name, type, OptionalInt.empty(), decode(signature));
    }

    private static Type decode(TypeSignature signature) {
        List<TypeSignature.TypeParameter> parameters = signature.parameters();
        return switch (signature.base()) {
            case "ARRAY" -> new ArrayType(decode(parameters.get(0).typeSignature()));
            case "MAP" -> new MapType(decode(parameters.get(0).typeSignature()),
                    decode(parameters.get(1).typeSignature()));
            case "ROW", "STRUCT" -> new RowType(parameters.stream().map(parameter ->
                    new RowField(parameter.fieldName(), decode(parameter.typeSignature()))).toList());
            case "VECTOR" -> new VectorType(Math.toIntExact(parameters.get(0).longValue()),
                    VectorType.ElementType.valueOf(parameters.get(1).typeSignature().base()));
            case "DECIMAL" -> new DecimalType(Math.toIntExact(parameters.get(0).longValue()),
                    Math.toIntExact(parameters.get(1).longValue()));
            case "VARCHAR" -> new VarcharType(Math.toIntExact(parameters.get(0).longValue()));
            case "CHAR" -> new CharType(Math.toIntExact(parameters.get(0).longValue()));
            case "REAL" -> RealType.INSTANCE;
            case "INTERVAL_DAY_TIME" -> IntervalDayTimeType.INSTANCE;
            case "INTERVAL_YEAR_MONTH" -> IntervalYearMonthType.INSTANCE;
            default -> PangrovaTypeMapping.toType(PangrovaType.fromSqlTypeName(signature.base()));
        };
    }
}
