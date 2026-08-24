/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.http;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Server-side idempotency registry mapping a client {@code Idempotency-Key} to the {@code queryId}
 * that first handled it (A-CLIENT-01). A retried write submission carrying the same key resolves to
 * the SAME query instead of executing a second time, closing the end-to-end idempotency gap left by
 * the client already generating the key while the server ignored it.
 *
 * <p>Entries expire after a TTL; the map is bounded by (write-rate x TTL) and a lazy sweep evicts
 * expired entries once it grows past a soft cap. Resolution is atomic per key, so concurrent retries
 * cannot both be treated as "first".
 *
 * <p>This dedups within a single (leader) coordinator, which is the submission target in Pangrova's
 * leader-based HA model. A shared/JDBC-backed implementation can replace it via the same seam for
 * multi-active-coordinator topologies.
 */
public final class IdempotencyKeyRegistry {

    /** Outcome of resolving a key: the associated queryId and whether this was a retry. */
    public record Resolution(String queryId, boolean duplicate) {}

    private static final Duration DEFAULT_TTL = Duration.ofMinutes(10);
    private static final int SWEEP_THRESHOLD = 10_000;

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final long ttlMillis;
    private final Clock clock;

    public IdempotencyKeyRegistry() {
        this(DEFAULT_TTL, Clock.systemUTC());
    }

    public IdempotencyKeyRegistry(Duration ttl, Clock clock) {
        this.ttlMillis = ttl.toMillis();
        this.clock = clock;
    }

    /**
     * Atomically resolves an idempotency key. If a live (unexpired) mapping already exists, returns
     * it with {@code duplicate=true} and does NOT invoke the supplier. Otherwise stores and returns a
     * fresh queryId from {@code newQueryIdSupplier} with {@code duplicate=false}. An expired mapping
     * is treated as new.
     */
    public Resolution resolve(String key, Supplier<String> newQueryIdSupplier) {
        long now = clock.millis();
        if (entries.size() > SWEEP_THRESHOLD) {
            entries.values().removeIf(e -> e.expiresAtMillis() <= now);
        }
        boolean[] created = {false};
        Entry result = entries.compute(key, (k, existing) -> {
            if (existing != null && existing.expiresAtMillis() > now) {
                return existing;
            }
            created[0] = true;
            return new Entry(newQueryIdSupplier.get(), now + ttlMillis);
        });
        return new Resolution(result.queryId(), !created[0]);
    }

    private record Entry(String queryId, long expiresAtMillis) {}
}
