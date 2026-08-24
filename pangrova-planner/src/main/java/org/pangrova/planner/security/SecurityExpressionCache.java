/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.planner.security;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.spi.security.UserIdentity;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Caches parsed security expression results (row filters and column masks)
 * keyed by (expression_text, user_identity) to avoid re-parsing on every
 * row/column evaluation within the same query or across repeated queries.
 *
 * <p>Uses Caffeine LRU cache with configurable max size and TTL.
 * Thread-safe: all operations are safe for concurrent access.
 */
public final class SecurityExpressionCache {

    private static final int DEFAULT_MAX_SIZE = 1024;
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(5);
    private static final String EMPTY_SENTINEL = "__EMPTY__";

    private final Cache<CacheKey, String> cache;
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();

    public SecurityExpressionCache() {
        this(DEFAULT_MAX_SIZE, DEFAULT_TTL);
    }

    /**
     * Creates a cache with explicit size and TTL bounds.
     *
     * @param maxSize maximum number of cached security expressions
     * @param ttl time-to-live for cache entries after write
     */
    public SecurityExpressionCache(int maxSize, Duration ttl) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(ttl)
                .build();
    }

    /**
     * Gets a cached expression result or computes it if absent.
     *
     * @param expressionText the raw security expression text (policy filter or mask)
     * @param user           the user identity for context binding
     * @param parser         supplier that computes the parsed expression if not cached
     * @return the parsed expression string, or empty if the supplier returns empty
     */
    public Optional<String> getOrCompute(String expressionText, UserIdentity user,
                                         Supplier<Optional<String>> parser) {
        CacheKey key = new CacheKey(expressionText, user.principal(), user.groups().hashCode());
        String cached = cache.getIfPresent(key);
        if (cached != null) {
            hitCount.incrementAndGet();
            return EMPTY_SENTINEL.equals(cached) ? Optional.empty() : Optional.of(cached);
        }
        missCount.incrementAndGet();
        Optional<String> result = parser.get();
        cache.put(key, result.orElse(EMPTY_SENTINEL));
        return result;
    }

    /** Returns the number of cache hits since creation. */
    public long hitCount() {
        return hitCount.get();
    }

    /** Returns the number of cache misses since creation. */
    public long missCount() {
        return missCount.get();
    }

    /** Returns the current number of entries in the cache. */
    public long size() {
        return cache.estimatedSize();
    }

    /** Invalidates all cached entries. Useful when security policies change. */
    public void invalidateAll() {
        cache.invalidateAll();
        hitCount.set(0);
        missCount.set(0);
    }

    /**
     * Cache key combining expression text and user identity components.
     */
    record CacheKey(String expressionText, String principal, int groupsHash) {
        CacheKey {
            Objects.requireNonNull(expressionText, "expressionText must not be null");
            Objects.requireNonNull(principal, "principal must not be null");
        }
    }
}
