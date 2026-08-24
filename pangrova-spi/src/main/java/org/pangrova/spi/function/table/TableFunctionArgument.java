package org.pangrova.spi.function.table;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.ColumnValue;

import java.util.Objects;

/**
 * Describes a formal argument to a polymorphic table function.
 *
 * <p>Table function arguments may be scalar values or table descriptors.
 * This record captures the argument name, its expected type name, and
 * whether a default value exists.</p>
 *
 * @param name         argument name as it appears in the SQL call
 * @param typeName     expected type name (e.g. {@code "VARCHAR"}, {@code "INTEGER"})
 * @param required     whether the argument must be supplied by the caller
 * @param defaultValue default value when the argument is optional, or null
 */
@Experimental(reason = "G-SPI-08: table function (PTF) plugin SPI")
public record TableFunctionArgument(
        String name,
        String typeName,
        boolean required,
        ColumnValue defaultValue) {

    public TableFunctionArgument {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(typeName, "typeName must not be null");
    }

    /**
     * Convenience constructor for a required argument with no default value.
     */
    public TableFunctionArgument(String name, String typeName) {
        this(name, typeName, true, null);
    }
}
