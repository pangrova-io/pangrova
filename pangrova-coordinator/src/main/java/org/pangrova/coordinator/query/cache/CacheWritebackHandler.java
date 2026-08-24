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
package org.pangrova.coordinator.query.cache;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.planner.cache.CacheEntryMetadata;
import org.pangrova.planner.cache.CacheKey;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.QueryResultCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Encapsulates cache writeback logic: extracts referenced tables from the plan,
 * builds {@link CacheEntryMetadata}, and writes the query result to
 * {@link QueryResultCache} after successful execution.
 *
 * <p>Writeback is fire-and-forget — failures are logged but never propagate
 * to the query response path.</p>
 *
 * <p>Thread-safe: stateless aside from the injected cache reference.</p>
 */
@ThreadSafe
public final class CacheWritebackHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CacheWritebackHandler.class);

    private final QueryResultCache cache;

    public CacheWritebackHandler(QueryResultCache cache) {
        this.cache = Objects.requireNonNull(cache, "cache must not be null");
    }

    /**
     * Writes a query result to the cache with metadata for precise invalidation.
     *
     * @param cacheKey        the isolation-aware cache key
     * @param data            serialized columnar IPC result bytes
     * @param tableReferences pre-collected table references from the compiled plan
     * @param principal       the user who executed the query
     * @param policyVersion   the security policy version at execution time
     */
    public void writeback(CacheKey cacheKey, byte[] data, List<List<String>> tableReferences,
                          String principal, String policyVersion) {
        try {
            Set<String> tables = toQualifiedNames(tableReferences);
            CacheEntryMetadata metadata = new CacheEntryMetadata(
                    tables,
                    principal != null ? principal : "anonymous",
                    policyVersion != null ? policyVersion : "");
            CachedResult result = new CachedResult(data, Instant.now());
            cache.put(cacheKey, result, metadata);
            LOG.info("event=cache_writeback_complete key={} tables={} principal={}",
                    cacheKey, tables, principal);
        } catch (Exception e) {
            LOG.warn("event=cache_writeback_failed key={} error={}",
                    cacheKey, e.getMessage(), e);
        }
    }

    static Set<String> toQualifiedNames(List<List<String>> tableReferences) {
        Set<String> tables = new HashSet<>();
        for (List<String> ref : tableReferences) {
            tables.add(String.join(".", ref).toLowerCase());
        }
        return Set.copyOf(tables);
    }
}
