package org.pangrova.connector.api.index;

import org.pangrova.common.api.annotation.Experimental;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * SPI contract for reading column values from a data file.
 *
 * <p>Implementations bridge the indexing module to connector-specific data
 * formats (e.g. Parquet, ORC) to extract column values for index building.
 */
@Experimental
public interface ColumnValueReader extends AutoCloseable {

    /**
     * Reads the specified columns from a data file, returning an iterator
     * over rows where each row is a list of column values in the requested order.
     *
     * @param dataFilePath absolute path to the data file
     * @param columnNames  columns to read (order preserved in output rows)
     * @return iterator over rows of column values
     * @throws IOException              if the file cannot be read
     * @throws IllegalArgumentException if any column does not exist in the file
     */
    Iterator<List<Comparable<?>>> readColumns(String dataFilePath, List<String> columnNames)
            throws IOException;
}
