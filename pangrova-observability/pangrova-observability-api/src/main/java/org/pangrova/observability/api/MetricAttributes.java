package org.pangrova.observability.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable set of key-value pairs attached to a metric recording,
 * enabling dimensional metrics without string concatenation in metric names.
 *
 * <p>Keys are non-null strings; values are non-null strings. Duplicate keys
 * are resolved by last-write-wins. Iteration order matches insertion order.
 *
 * <p>Use the static factory methods to create instances:
 * <pre>{@code
 * MetricAttributes attrs = MetricAttributes.of("tenant", "acme", "region", "us-east-1");
 * counter.add(1, attrs);
 * }</pre>
 */
public final class MetricAttributes {

    private static final MetricAttributes EMPTY = new MetricAttributes(Collections.emptyMap());

    private final Map<String, String> entries;

    private MetricAttributes(Map<String, String> entries) {
        this.entries = entries;
    }

    /**
     * Returns an empty attributes instance.
     */
    public static MetricAttributes empty() {
        return EMPTY;
    }

    /**
     * Creates attributes from a single key-value pair.
     */
    public static MetricAttributes of(String key, String value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        return new MetricAttributes(Map.of(key, value));
    }

    /**
     * Creates attributes from two key-value pairs.
     */
    public static MetricAttributes of(String k1, String v1, String k2, String v2) {
        Objects.requireNonNull(k1, "k1 must not be null");
        Objects.requireNonNull(v1, "v1 must not be null");
        Objects.requireNonNull(k2, "k2 must not be null");
        Objects.requireNonNull(v2, "v2 must not be null");
        Map<String, String> map = new LinkedHashMap<>(4);
        map.put(k1, v1);
        map.put(k2, v2);
        return new MetricAttributes(Collections.unmodifiableMap(map));
    }

    /**
     * Creates attributes from three key-value pairs.
     */
    public static MetricAttributes of(String k1, String v1, String k2, String v2,
                                       String k3, String v3) {
        Objects.requireNonNull(k1, "k1 must not be null");
        Objects.requireNonNull(v1, "v1 must not be null");
        Objects.requireNonNull(k2, "k2 must not be null");
        Objects.requireNonNull(v2, "v2 must not be null");
        Objects.requireNonNull(k3, "k3 must not be null");
        Objects.requireNonNull(v3, "v3 must not be null");
        Map<String, String> map = new LinkedHashMap<>(6);
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        return new MetricAttributes(Collections.unmodifiableMap(map));
    }

    /**
     * Creates attributes from an arbitrary map of key-value pairs.
     *
     * @param entries the key-value pairs; must not contain null keys or values
     * @return a new immutable MetricAttributes instance
     */
    public static MetricAttributes fromMap(Map<String, String> entries) {
        Objects.requireNonNull(entries, "entries must not be null");
        if (entries.isEmpty()) {
            return EMPTY;
        }
        Map<String, String> copy = new LinkedHashMap<>(entries.size());
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            Objects.requireNonNull(entry.getKey(), "attribute key must not be null");
            Objects.requireNonNull(entry.getValue(), "attribute value must not be null");
            copy.put(entry.getKey(), entry.getValue());
        }
        return new MetricAttributes(Collections.unmodifiableMap(copy));
    }

    /**
     * Returns an unmodifiable view of the key-value pairs.
     */
    public Map<String, String> asMap() {
        return entries;
    }

    /**
     * Returns true if this instance contains no attributes.
     */
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * Returns the number of key-value pairs.
     */
    public int size() {
        return entries.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricAttributes that)) return false;
        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        return "MetricAttributes" + entries;
    }
}
