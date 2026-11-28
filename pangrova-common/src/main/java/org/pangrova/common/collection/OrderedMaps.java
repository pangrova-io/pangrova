package org.pangrova.common.collection;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Map snapshots whose iteration order is part of the caller's contract. */
public final class OrderedMaps {
    private OrderedMaps() {}

    /** Returns an unmodifiable shallow copy in source iteration order, retaining null keys and values. */
    public static <K, V> Map<K, V> immutableCopy(Map<? extends K, ? extends V> source) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }
}
