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

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for A-CLIENT-01 server-side idempotency dedup ({@link IdempotencyKeyRegistry}).
 */
class IdempotencyKeyRegistryTest {

    @Test
    void firstKeyIsNotDuplicate_andRetryReturnsSameQueryIdWithoutInvokingSupplier() {
        IdempotencyKeyRegistry registry = new IdempotencyKeyRegistry();
        AtomicInteger supplierCalls = new AtomicInteger();

        IdempotencyKeyRegistry.Resolution first =
                registry.resolve("key-A", () -> "q-" + supplierCalls.incrementAndGet());
        assertThat(first.duplicate()).isFalse();
        assertThat(first.queryId()).isEqualTo("q-1");

        // Retry with the same key must resolve to the same queryId and NOT invoke the supplier.
        IdempotencyKeyRegistry.Resolution retry =
                registry.resolve("key-A", () -> "q-" + supplierCalls.incrementAndGet());
        assertThat(retry.duplicate()).isTrue();
        assertThat(retry.queryId()).isEqualTo("q-1");
        assertThat(supplierCalls.get()).as("supplier must not run for a duplicate").isEqualTo(1);
    }

    @Test
    void differentKeysResolveIndependently() {
        IdempotencyKeyRegistry registry = new IdempotencyKeyRegistry();
        IdempotencyKeyRegistry.Resolution a = registry.resolve("A", () -> "q-A");
        IdempotencyKeyRegistry.Resolution b = registry.resolve("B", () -> "q-B");
        assertThat(a.duplicate()).isFalse();
        assertThat(b.duplicate()).isFalse();
        assertThat(a.queryId()).isEqualTo("q-A");
        assertThat(b.queryId()).isEqualTo("q-B");
    }

    @Test
    void expiredKeyIsTreatedAsNew() {
        TestClock clock = new TestClock(Instant.parse("2026-01-01T00:00:00Z"));
        IdempotencyKeyRegistry registry = new IdempotencyKeyRegistry(Duration.ofMillis(100), clock);

        IdempotencyKeyRegistry.Resolution first = registry.resolve("k", () -> "q-1");
        assertThat(first.duplicate()).isFalse();

        // Within TTL: still a duplicate.
        clock.advance(Duration.ofMillis(50));
        assertThat(registry.resolve("k", () -> "q-2").duplicate()).isTrue();

        // Past TTL: treated as new, and a fresh queryId is issued.
        clock.advance(Duration.ofMillis(100));
        IdempotencyKeyRegistry.Resolution afterExpiry = registry.resolve("k", () -> "q-3");
        assertThat(afterExpiry.duplicate()).isFalse();
        assertThat(afterExpiry.queryId()).isEqualTo("q-3");
    }

    /** Minimal advanceable clock for deterministic TTL testing. */
    private static final class TestClock extends Clock {
        private Instant now;

        TestClock(Instant start) {
            this.now = start;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public long millis() {
            return now.toEpochMilli();
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
