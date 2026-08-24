package org.pangrova.spi.connector.data.type;

import java.util.List;
import java.util.Objects;

import org.pangrova.spi.annotation.Experimental;

/**
 * Immutable value object representing a serializable type signature.
 *
 * <p>A type signature captures the base type name and any type parameters,
 * enabling round-trip serialization/deserialization of parameterized types
 * such as {@code DECIMAL(18,2)}, {@code ARRAY(VARCHAR(255))}, or
 * {@code ROW(name VARCHAR(100), age INTEGER)}.</p>
 *
 * <p>This class is thread-safe and immutable.</p>
 *
 * @param base       the base type name (e.g. "BOOLEAN", "DECIMAL", "ARRAY")
 * @param parameters ordered list of type parameters (empty for non-parameterized types)
 */
@Experimental
public record TypeSignature(String base, List<TypeParameter> parameters) {

    public TypeSignature {
        Objects.requireNonNull(base, "base must not be null");
        parameters = List.copyOf(parameters);
    }

    /**
     * Creates a signature with no parameters.
     */
    public TypeSignature(String base) {
        this(base, List.of());
    }

    /**
     * A single type parameter — either a long literal or a nested type signature.
     *
     * @param longValue     numeric parameter value (e.g. precision, scale, length); null if type parameter
     * @param typeSignature nested type signature (e.g. element type of ARRAY); null if numeric parameter
     * @param fieldName     optional field name for ROW fields; null otherwise
     */
    public record TypeParameter(Long longValue, TypeSignature typeSignature, String fieldName) {

        /**
         * Creates a numeric parameter (precision, scale, length).
         */
        public static TypeParameter ofLong(long value) {
            return new TypeParameter(value, null, null);
        }

        /**
         * Creates a nested type parameter.
         */
        public static TypeParameter ofType(TypeSignature signature) {
            Objects.requireNonNull(signature, "signature must not be null");
            return new TypeParameter(null, signature, null);
        }

        /**
         * Creates a named field parameter (for ROW types).
         */
        public static TypeParameter ofNamedType(String name, TypeSignature signature) {
            Objects.requireNonNull(name, "name must not be null");
            Objects.requireNonNull(signature, "signature must not be null");
            return new TypeParameter(null, signature, name);
        }
    }
}
