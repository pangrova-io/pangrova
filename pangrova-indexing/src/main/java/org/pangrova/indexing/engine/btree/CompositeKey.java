package org.pangrova.indexing.engine.btree;

import java.util.List;

/**
 * Composite key for multi-column BTree indexes.
 *
 * <p>Compares column values lexicographically in column order.
 * For single-column indexes, delegates directly to the single value's compareTo.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
final class CompositeKey implements Comparable<CompositeKey> {

    private final List<Comparable<?>> components;

    CompositeKey(List<Comparable<?>> components) {
        this.components = List.copyOf(components);
    }

    List<Comparable<?>> components() {
        return components;
    }

    @Override
    public int compareTo(CompositeKey other) {
        int len = Math.min(components.size(), other.components.size());
        for (int i = 0; i < len; i++) {
            int cmp = ((Comparable) components.get(i)).compareTo(other.components.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(components.size(), other.components.size());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CompositeKey other)) return false;
        return components.equals(other.components);
    }

    @Override
    public int hashCode() {
        return components.hashCode();
    }

    @Override
    public String toString() {
        return "CompositeKey" + components;
    }
}
