package org.pangrova.spi.function.table;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;

import java.util.Iterator;
import java.util.List;

/**
 * A user-defined polymorphic table function (PTF): accepts scalar arguments
 * and produces a row stream.
 *
 * <p>Table functions appear in the {@code FROM} clause of a query:
 * <pre>
 *   SELECT * FROM TABLE(my_ptf(arg1 =&gt; 'value'))
 * </pre>
 *
 * <p>Implementations must be thread-safe at the factory level (multiple
 * concurrent calls to {@link #execute}), but each {@link Iterator} returned
 * by {@link #execute} is consumed by a single thread.</p>
 *
 * @see TableFunctionFactory
 */
@Experimental(reason = "G-SPI-08: table function (PTF) plugin SPI")
public interface TableFunction {

    /**
     * Returns the function name as it appears in SQL.
     *
     * @return function name, never null
     */
    String getName();

    /**
     * Returns the formal arguments accepted by this function.
     *
     * @return list of argument descriptors, never null (may be empty)
     */
    List<TableFunctionArgument> getArguments();

    /**
     * Returns the column descriptors for the result set.
     *
     * <p>The descriptor defines the output schema of the table function.
     * All rows returned by {@link #execute} must conform to this schema.</p>
     *
     * @return list of column descriptors, never null and never empty
     */
    List<ReturnColumnDescriptor> getReturnType();

    /**
     * Executes the table function with the given argument values.
     *
     * <p>The returned iterator produces rows lazily. The engine will consume
     * the iterator in a single thread and close it when done (if it implements
     * {@link java.io.Closeable}).</p>
     *
     * @param arguments positional argument values matching the declared arguments
     * @return an iterator over the result rows, never null
     */
    Iterator<TypedRow> execute(List<ColumnValue> arguments);
}
