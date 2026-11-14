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
package org.pangrova.server.coordinator.http.request;

import jakarta.ws.rs.ClientErrorException;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.client.result.ResultEndpoint;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotencyKeyRegistryTest {
    private static final SubmitQueryRequest REQUEST = new SubmitQueryRequest("SELECT 1", Map.of());
    private static final SubmitQueryResponse RECEIPT = new SubmitQueryResponse("q-1", "FINISHED",
            new ResultEndpoint("worker", 10, "original-ticket", "flight"), List.of("column"));

    @Test
    void sharesFullReceiptAndSeparatesPrincipalsAndKeys() {
        var registry = new IdempotencyKeyRegistry();
        var calls = new AtomicInteger();
        var next = (java.util.function.Supplier<SubmitQueryResponse>) () -> {
            calls.incrementAndGet();
            return RECEIPT;
        };
        assertThat(registry.submit("alice", "A", REQUEST, next)).isSameAs(RECEIPT);
        assertThat(registry.submit("alice", "A", REQUEST, next)).isSameAs(RECEIPT);
        registry.submit("bob", "A", REQUEST, next);
        registry.submit("alice", "B", REQUEST, next);
        assertThat(calls).hasValue(3);
    }

    @Test
    void validatesCompleteRequestWithoutConcatenatingProperties() {
        var registry = new IdempotencyKeyRegistry();
        registry.submit("alice", "key", new SubmitQueryRequest("SELECT 1", Map.of("ab", "c")), () -> RECEIPT);
        assertThatThrownBy(() -> registry.submit("alice", "key",
                new SubmitQueryRequest("SELECT 1", Map.of("a", "bc")), () -> RECEIPT))
                .isInstanceOf(ClientErrorException.class);
    }

    @Test
    void ttlStartsWhenSubmissionFinishesAndConcurrentRetrySharesResult() throws Exception {
        var clock = new TestClock(Instant.EPOCH);
        var registry = new IdempotencyKeyRegistry(Duration.ofMillis(100), clock);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            var first = pool.submit(() -> registry.submit("alice", "key", REQUEST, () -> {
                started.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("first submission never released");
                    }
                } catch (InterruptedException interrupted) {
                    throw new AssertionError(interrupted);
                }
                return RECEIPT;
            }));
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            clock.advance(Duration.ofMillis(200));
            assertThatThrownBy(() -> registry.submit("alice", "key",
                    new SubmitQueryRequest("SELECT 2", Map.of()), () -> RECEIPT))
                    .isInstanceOf(ClientErrorException.class);
            var retry = pool.submit(() -> registry.submit("alice", "key", REQUEST, () -> {
                throw new AssertionError("in-flight submission repeated");
            }));
            release.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS)).isSameAs(RECEIPT);
            assertThat(retry.get(10, TimeUnit.SECONDS)).isSameAs(RECEIPT);
            clock.advance(Duration.ofMillis(101));
            var fresh = new SubmitQueryResponse("q-2", "RUNNING", null, List.of());
            assertThat(registry.submit("alice", "key", REQUEST, () -> fresh)).isSameAs(fresh);
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void rejectedBeforeRegistrationCanRetryButRecordedFailureIsReplayed() {
        var registry = new IdempotencyKeyRegistry();
        assertThatThrownBy(() -> registry.submit("alice", "key", REQUEST, () -> {
            throw new IllegalStateException("temporarily unavailable");
        })).hasMessage("temporarily unavailable");
        var failure = new IdempotencyKeyRegistry.RecordedSubmissionFailure("q-1",
                new IllegalStateException("already registered"));
        assertThatThrownBy(() -> registry.submit("alice", "key", REQUEST, () -> { throw failure; }))
                .isSameAs(failure);
        assertThatThrownBy(() -> registry.submit("alice", "key", REQUEST, () -> RECEIPT)).isSameAs(failure);
    }

    /** Minimal advanceable clock for deterministic TTL testing. */
    private static final class TestClock extends Clock {
        private volatile Instant now;

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
