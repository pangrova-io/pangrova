package org.pangrova.indexing.api.data;

import org.pangrova.common.api.annotation.Experimental;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Immutable, complete multi-column index key in declared column order.
 * Components are non-null scalar values supported by the comparable codec:
 * String, boxed numeric primitives, BigDecimal, BigInteger and UUID (exact classes).
 * Nested keys and mutable/custom Comparable implementations are rejected.
 * Comparison requires the same arity and component types; it is lexicographic,
 * not a prefix comparison or SQL predicate binding. No external resource is retained.
 */
@Experimental
public record CompositeIndexKey(List<Comparable<?>> components) implements Comparable<CompositeIndexKey> {
    public static final int MAX_COMPONENTS = 1024;
    private static final Set<Class<?>> SCALAR_TYPES = Set.of(String.class, Integer.class, Long.class,
            Double.class, Float.class, Short.class, Byte.class, BigDecimal.class, BigInteger.class, UUID.class);

    public CompositeIndexKey {
        components = List.copyOf(components);
        if (components.size() < 2 || components.size() > MAX_COMPONENTS) {
            throw new IllegalArgumentException("Composite index key requires 2.." + MAX_COMPONENTS + " components");
        }
        for (Comparable<?> component : components) {
            if (!SCALAR_TYPES.contains(component.getClass())) {
                throw new IllegalArgumentException(
                        "Unsupported composite index component: " + component.getClass().getName());
            }
        }
    }

    /** Whether comparison is defined without coercion or a prefix assumption. */
    public boolean hasSameShape(CompositeIndexKey other) {
        if (other == null || components.size() != other.components.size()) return false;
        for (int i = 0; i < components.size(); i++) {
            if (components.get(i).getClass() != other.components.get(i).getClass()) return false;
        }
        return true;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int compareTo(CompositeIndexKey other) {
        if (!hasSameShape(other)) throw new IllegalArgumentException("Incompatible composite index key shape");
        for (int i = 0; i < components.size(); i++) {
            int comparison = ((Comparable) components.get(i)).compareTo(other.components.get(i));
            if (comparison != 0) return comparison;
        }
        return 0;
    }
}
