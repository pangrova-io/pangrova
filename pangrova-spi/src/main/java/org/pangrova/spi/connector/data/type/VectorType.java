package org.pangrova.spi.connector.data.type;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Fixed-dimension floating-point vector type, analogous to {@code DECIMAL(precision, scale)}
 * as a parameterized composite type.
 *
 * <p>Represents a column that stores dense vectors of a fixed dimension, used for
 * similarity search (ANN) workloads. The element type determines storage precision:
 * {@link ElementType#FLOAT} (4 bytes) or {@link ElementType#DOUBLE} (8 bytes).</p>
 *
 * <p>SQL syntax: {@code VECTOR(768)} (default FLOAT) or {@code VECTOR(384, DOUBLE)}.</p>
 *
 * <p>This record is immutable and thread-safe.</p>
 *
 * @param dimension   number of elements per vector, in range [1, 65535]
 * @param elementType scalar element precision; defaults to {@link ElementType#FLOAT} if null
 */
@Experimental
public record VectorType(int dimension, ElementType elementType) implements Type {

    /** Scalar element precision for vector components. */
    public enum ElementType { FLOAT, DOUBLE }

    private static final int MIN_DIMENSION = 1;
    private static final int MAX_DIMENSION = 65_535;
    private static final int FLOAT_BYTES = 4;
    private static final int DOUBLE_BYTES = 8;

    public VectorType {
        if (dimension < MIN_DIMENSION || dimension > MAX_DIMENSION) {
            throw new IllegalArgumentException(
                    "dimension must be in [" + MIN_DIMENSION + ", " + MAX_DIMENSION + "], got: " + dimension);
        }
        if (elementType == null) {
            elementType = ElementType.FLOAT;
        }
    }

    /**
     * Returns the number of bytes required to store one vector.
     */
    public int bytesPerVector() {
        return dimension * (elementType == ElementType.FLOAT ? FLOAT_BYTES : DOUBLE_BYTES);
    }

    @Override
    public String getDisplayName() {
        return elementType == ElementType.FLOAT
                ? "VECTOR(" + dimension + ")"
                : "VECTOR(" + dimension + "," + elementType + ")";
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature("VECTOR", List.of(
                TypeSignature.TypeParameter.ofLong(dimension),
                TypeSignature.TypeParameter.ofType(new TypeSignature(elementType.name()))));
    }
}
