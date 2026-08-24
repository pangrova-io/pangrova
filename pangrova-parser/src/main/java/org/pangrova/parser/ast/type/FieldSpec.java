package org.pangrova.parser.ast.type;

import java.util.Objects;

/**
 * A named field within a STRUCT type definition.
 *
 * @param name the field name
 * @param type the field's data type specification
 */
public record FieldSpec(String name, DataTypeSpec type) {

    public FieldSpec {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(type, "type must not be null");
    }

    /**
     * Returns the canonical string representation of this field (e.g. "city VARCHAR").
     */
    public String toCanonicalString() {
        return name + " " + type.toCanonicalString();
    }
}
