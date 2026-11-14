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
package org.pangrova.planner.analysis;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Caching decorator for {@link SubPlanReoptimizer} that memoizes reoptimization
 * results keyed by plan digest. Avoids redundant re-optimization for structurally
 * equivalent sub-plans within the same query execution.
 *
 * <p>Uses a bounded LRU cache to prevent unbounded memory growth.
 * Not thread-safe — intended for single-query-execution scope.</p>
 */
public final class CachingSubPlanReoptimizer implements SubPlanReoptimizer {

    private static final int DEFAULT_CACHE_SIZE = 64;

    private final SubPlanReoptimizer delegate;
    private final Map<String, PlanFragment> cache;

    public CachingSubPlanReoptimizer(SubPlanReoptimizer delegate) {
        this(delegate, DEFAULT_CACHE_SIZE);
    }

    /**
     * Wraps the delegate with an LRU re-optimization cache of the given size.
     *
     * @param delegate underlying reoptimizer
     * @param maxSize maximum number of cached plan fragments
     */
    public CachingSubPlanReoptimizer(SubPlanReoptimizer delegate, int maxSize) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.cache = new LinkedHashMap<>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, PlanFragment> eldest) {
                return size() > maxSize;
            }
        };
    }

    @Override
    public PlanFragment reoptimize(PlanFragment fragment) {
        String digest = fragment.planNode.explain();
        PlanFragment cached = cache.get(digest);
        if (cached != null) {
            return cached;
        }
        PlanFragment result = delegate.reoptimize(fragment);
        cache.put(digest, result);
        return result;
    }

    @Override
    public PlanFragment reoptimize(PlanFragment fragment,
            org.pangrova.planner.api.enums.JoinStrategy joinStrategyHint) {
        // Hint-aware variant skips the digest cache because identical fragments
        // with different hints can yield different plans (F-AQE-03).
        return delegate.reoptimize(fragment, joinStrategyHint);
    }
}
