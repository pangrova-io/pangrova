package org.pangrova.indexing.core.config;

/**
 * Groups runtime behavior configuration: build, cache, and evaluation.
 *
 * @param build build configuration
 * @param cache cache configuration
 * @param eval  evaluation configuration
 */
public record RuntimeConfig(BuildConfig build, CacheConfig cache, EvalConfig eval) {

    public static final RuntimeConfig DEFAULT =
            new RuntimeConfig(BuildConfig.DEFAULT, CacheConfig.DEFAULT, EvalConfig.DEFAULT);

    public RuntimeConfig {
        if (build == null) build = BuildConfig.DEFAULT;
        if (cache == null) cache = CacheConfig.DEFAULT;
        if (eval == null) eval = EvalConfig.DEFAULT;
    }
}
