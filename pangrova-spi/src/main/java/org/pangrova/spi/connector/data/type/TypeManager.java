package org.pangrova.spi.connector.data.type;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;

/**
 * Registry and utility interface for the Pangrova parameterized type system.
 *
 * <p>Responsible for type registration, compatibility checking, and common
 * super-type derivation. Implementations must be thread-safe.</p>
 */
@Experimental
public interface TypeManager {

    /**
     * Resolves a {@link TypeSignature} to a concrete {@link Type} instance.
     *
     * @param signature the type signature to resolve
     * @return the resolved type
     * @throws IllegalArgumentException if the signature cannot be resolved
     */
    Type getType(TypeSignature signature);

    /**
     * Checks whether a value of {@code source} type can be assigned to a
     * target of {@code target} type without data loss.
     *
     * @param source the source type
     * @param target the target type
     * @return true if the assignment is compatible
     */
    boolean isCompatible(Type source, Type target);

    /**
     * Derives the narrowest common super-type of two types, if one exists.
     *
     * @param a first type
     * @param b second type
     * @return the common super-type, or empty if the types are incompatible
     */
    Optional<Type> getCommonSuperType(Type a, Type b);
}
