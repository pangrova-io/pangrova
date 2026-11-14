package org.pangrova.cache.api;

import java.util.Objects;
import java.util.Optional;

/** A single atomic observation. A miss still carries the generations needed to admit a later fill. */
public record CacheLookup(CacheKey key, String groupGeneration, String entryGeneration, Optional<String> value) {
    public CacheLookup {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(groupGeneration, "groupGeneration");
        Objects.requireNonNull(entryGeneration, "entryGeneration");
        Objects.requireNonNull(value, "value");
    }
}
