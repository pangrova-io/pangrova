package org.pangrova.spi.connector.data.type;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;
import java.util.Objects;

/**
 * Immutable connector type declaration with recursive nullability.
 *
 * <p>Children follow the ordered structural type parameters. Execution-specific
 * projection and supported-type validation belong to the protocol consumer;
 * this neutral declaration contains no runtime or storage-framework types.</p>
 *
 * @param signature complete structural type identity
 * @param nullable whether the declared value allows null
 * @param children ordered declarations for the signature's structural children
 */
@Experimental
public record DeclaredType(TypeSignature signature, boolean nullable, List<DeclaredType> children) {
    private static final int MAX_NESTED_TYPES = 4096;

    /**
     * Creates an immutable declaration without choosing execution-specific type support.
     *
     * @throws NullPointerException if the signature, children or any child is null
     * @throws IllegalArgumentException if the child count exceeds the declaration bound
     */
    public DeclaredType {
        Objects.requireNonNull(signature, "signature");
        children = List.copyOf(children);
        if (children.size() > MAX_NESTED_TYPES) {
            throw new IllegalArgumentException("Too many nested physical types");
        }
    }
}
