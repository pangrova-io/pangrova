package org.pangrova.connector.paimon.schema;

import java.util.List;
import java.util.Map;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VectorType;
import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.spi.connector.data.type.TypeSignature;
import org.pangrova.spi.connector.data.type.DeclaredType;

/** Immutable complete SDK type metadata for the unified physical write contract. */
final class PaimonDeclaredMetadata {
    private PaimonDeclaredMetadata() {}

    static DeclaredType declared(DataType type, String name, Map<String, String> options) {
        TypeSignature signature = PaimonTypeMapper.toParameterizedType(type, name, options).getTypeSignature();
        var children = children(type);
        if (type instanceof TimestampType timestamp) {
            signature = precision("TIMESTAMP", timestamp.getPrecision());
        } else if (type instanceof LocalZonedTimestampType timestamp) {
            signature = precision("TIMESTAMP_TZ", timestamp.getPrecision());
        }
        if ("ARRAY".equals(signature.base()) || "MAP".equals(signature.base())) {
            signature = new TypeSignature(signature.base(), children.stream()
                    .map(child -> TypeSignature.TypeParameter.ofType(child.signature())).toList());
        } else if (type instanceof RowType row) {
            var fields = new java.util.ArrayList<TypeSignature.TypeParameter>(children.size());
            for (int index = 0; index < children.size(); index++) {
                fields.add(TypeSignature.TypeParameter.ofNamedType(row.getFieldNames().get(index),
                        children.get(index).signature()));
            }
            signature = new TypeSignature("ROW", fields);
        }
        return new DeclaredType(signature, type.isNullable(), children);
    }

    private static List<DeclaredType> children(DataType type) {
        if (type instanceof ArrayType array) {
            return List.of(declared(array.getElementType(), "", Map.of()));
        }
        if (type instanceof VectorType vector) {
            return List.of(declared(vector.getElementType(), "", Map.of()));
        }
        if (type instanceof MapType map) {
            return List.of(declared(map.getKeyType(), "", Map.of()), declared(map.getValueType(), "", Map.of()));
        }
        if (type instanceof RowType row) {
            return row.getFields().stream().map(field -> declared(field.type(), field.name(), Map.of())).toList();
        }
        return List.of();
    }

    private static TypeSignature precision(String base, int precision) {
        return new TypeSignature(base, List.of(TypeSignature.TypeParameter.ofLong(precision)));
    }
}
