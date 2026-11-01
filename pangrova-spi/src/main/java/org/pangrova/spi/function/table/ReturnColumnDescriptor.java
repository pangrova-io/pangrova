package org.pangrova.spi.function.table;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/**
 * Describes a single column in the result set of a table function.
 *
 * @param name     column name
 * @param typeName column type name (e.g. {@code "VARCHAR"}, {@code "BIGINT"})
 */
@Experimental(reason = "G-SPI-08: table function (PTF) plugin SPI")
public record ReturnColumnDescriptor(String name, String typeName) {

    public ReturnColumnDescriptor {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(typeName, "typeName must not be null");
    }
}
