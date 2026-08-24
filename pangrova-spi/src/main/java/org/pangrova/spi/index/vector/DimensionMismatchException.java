package org.pangrova.spi.index.vector;

/**
 * Thrown when a vector's dimension does not match the column definition.
 *
 * <p>This can occur during INSERT/UPDATE when the provided vector has a
 * different number of elements than the column's declared dimension, or
 * during search when the query vector dimension differs from the index.</p>
 */
public class DimensionMismatchException extends RuntimeException {

    public DimensionMismatchException(String message) {
        super(message);
    }

    public DimensionMismatchException(int expected, int actual) {
        super("Expected vector dimension " + expected + " but got " + actual);
    }
}
