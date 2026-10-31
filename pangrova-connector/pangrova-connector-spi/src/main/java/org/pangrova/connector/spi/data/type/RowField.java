package org.pangrova.connector.spi.data.type;

import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * A named field within a row type.
 *
 * @param name the field name
 * @param type the field type
 */
@Experimental
public record RowField(String name, Type type) {

    public RowField {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(type, "type must not be null");
    }
}
