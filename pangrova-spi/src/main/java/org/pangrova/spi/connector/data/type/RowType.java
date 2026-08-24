package org.pangrova.spi.connector.data.type;

import java.util.List;
import java.util.stream.Collectors;

import org.pangrova.spi.annotation.Experimental;

/**
 * Structured type with named fields.
 *
 * @param fields ordered list of named fields
 */
@Experimental
public record RowType(List<RowField> fields) implements Type {

    public RowType {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("RowType must have at least one field");
        }
        fields = List.copyOf(fields);
    }

    @Override
    public String getDisplayName() {
        String fieldList = fields.stream()
                .map(f -> f.name() + " " + f.type().getDisplayName())
                .collect(Collectors.joining(","));
        return "ROW(" + fieldList + ")";
    }

    @Override
    public TypeSignature getTypeSignature() {
        List<TypeSignature.TypeParameter> params = fields.stream()
                .map(f -> TypeSignature.TypeParameter.ofNamedType(
                        f.name(), f.type().getTypeSignature()))
                .toList();
        return new TypeSignature("ROW", params);
    }
}
