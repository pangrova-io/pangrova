package org.pangrova.cache.api;

import java.util.Objects;

/** Variants share invalidation, but never share values. Groups can be invalidated atomically. */
public record CacheKey(String group, String entry, String variant) {
    public CacheKey {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(variant, "variant");
    }
}
