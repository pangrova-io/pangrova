package org.pangrova.connector.paimon.type;

import java.util.ArrayList;
import java.util.List;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.MapType;
import org.pangrova.spi.connector.data.type.PangrovaTypeMapping;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.data.type.primitive.CharType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;

/** Converts complete declared types without discarding nested or scalar parameters. */
public final class PaimonDeclaredTypes {
    private PaimonDeclaredTypes() {}

    /**
     * Converts a complete declaration using the table's selected vector representation.
     *
     * @param type the complete declared type
     * @param nativeVector whether the table uses the SDK native vector type
     * @return the exact SDK type, including nested parameters
     */
    public static DataType toPaimonType(Type type, boolean nativeVector) {
        if (type instanceof ArrayType array) {
            return new org.apache.paimon.types.ArrayType(toPaimonType(array.elementType(), nativeVector));
        }
        if (type instanceof MapType map) {
            return new org.apache.paimon.types.MapType(toPaimonType(map.keyType(), nativeVector),
                    toPaimonType(map.valueType(), nativeVector));
        }
        if (type instanceof RowType row) {
            return toPaimonRow(row, nativeVector);
        }
        if (type instanceof VectorType vector) {
            return nativeVector
                    ? new org.apache.paimon.types.VectorType(vector.dimension(),
                            PaimonTypeMapper.toPaimonVectorType(vector).getElementType())
                    : PaimonTypeMapper.toPaimonVectorType(vector);
        }
        return toPaimonScalar(type);
    }

    private static DataType toPaimonRow(RowType row, boolean nativeVector) {
        List<DataField> fields = new ArrayList<>(row.fields().size());
        for (int index = 0; index < row.fields().size(); index++) {
            var field = row.fields().get(index);
            fields.add(new DataField(index, field.name(), toPaimonType(field.type(), nativeVector)));
        }
        // Schema.Builder assigns table-wide immutable IDs, including all nested fields.
        return new org.apache.paimon.types.RowType(fields);
    }

    private static DataType toPaimonScalar(Type type) {
        if (type instanceof DecimalType decimal) {
            return new org.apache.paimon.types.DecimalType(decimal.precision(), decimal.scale());
        }
        if (type instanceof VarcharType varchar) {
            return new org.apache.paimon.types.VarCharType(varchar.length());
        }
        if (type instanceof CharType character) {
            return new org.apache.paimon.types.CharType(character.length());
        }
        return PaimonTypeMapper.toPaimonType(PangrovaTypeMapping.toPangrovaType(type));
    }
}
