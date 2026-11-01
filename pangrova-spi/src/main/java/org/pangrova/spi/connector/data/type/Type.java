package org.pangrova.spi.connector.data.type;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Interface representing a parameterized type in the Pangrova type system.
 *
 * <p>Provides a rich type hierarchy that supports parameterized types
 * (e.g. {@code DECIMAL(18,2)}, {@code VARCHAR(255)}), complex types
 * ({@code ARRAY}, {@code MAP}, {@code ROW}), and type-safe operations.</p>
 *
 * <p>All implementations are immutable records, ensuring thread safety
 * and value semantics.</p>
 *
 * @see TypeSignature
 * @see TypeManager
 */
@Experimental
public interface Type {

    /**
     * Returns a human-readable display name for this type.
     * For parameterized types, includes parameters (e.g. "DECIMAL(18,2)").
     */
    String getDisplayName();

    /**
     * Returns the serializable type signature for round-trip serialization.
     */
    TypeSignature getTypeSignature();
}
