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

import org.pangrova.server.coordinator.exception.RecordedSubmissionException;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
        var next = (java.util.function.Supplier<CompletionStage<SubmitQueryResponse>>) () -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(RECEIPT);
        };
        assertThat(registry.submit("alice", "A", REQUEST, next).toCompletableFuture()).isCompletedWithValue(RECEIPT);
        assertThat(registry.submit("alice", "A", REQUEST, next).toCompletableFuture()).isCompletedWithValue(RECEIPT);
        registry.submit("bob", "A", REQUEST, next);
        registry.submit("alice", "B", REQUEST, next);
        assertThat(calls).hasValue(3);
    }

    @Test
    void validatesCompleteRequestWithoutConcatenatingProperties() {
        var registry = new IdempotencyKeyRegistry();
        registry.submit("alice", "key", new SubmitQueryRequest("SELECT 1", Map.of("ab", "c")), () -> CompletableFuture.completedFuture(RECEIPT));
        assertThatThrownBy(() -> registry.submit("alice", "key",
                new SubmitQueryRequest("SELECT 1", Map.of("a", "bc")), () -> CompletableFuture.completedFuture(RECEIPT)))
                .isInstanceOf(ClientErrorException.class);
    }

    @Test
    void ttlStartsWhenSubmissionFinishesAndConcurrentObserversShareTheOwnedReceipt() {
        var clock = new TestClock(Instant.EPOCH);
        var registry = new IdempotencyKeyRegistry(Duration.ofMillis(100), clock);
        var owned = new CompletableFuture<SubmitQueryResponse>();
        var first = registry.submit("alice", "key", REQUEST, () -> owned).toCompletableFuture();
        clock.advance(Duration.ofMillis(200));
        assertThatThrownBy(() -> registry.submit("alice", "key",
                new SubmitQueryRequest("SELECT 2", Map.of()), () -> CompletableFuture.completedFuture(RECEIPT)))
                .isInstanceOf(ClientErrorException.class);
        var retry = registry.submit("alice", "key", REQUEST, () -> {
            throw new AssertionError("in-flight submission repeated");
        }).toCompletableFuture();
        var disconnected = registry.submit("alice", "key", REQUEST, () -> {
            throw new AssertionError("in-flight submission repeated");
        }).toCompletableFuture();
        assertThat(disconnected.cancel(false)).isTrue();
        assertThat(owned).isNotDone();
        assertThat(first).isNotDone();
        assertThat(retry).isNotDone();
        owned.complete(RECEIPT);
        assertThat(first).isCompletedWithValue(RECEIPT);
        assertThat(retry).isCompletedWithValue(RECEIPT);

        clock.advance(Duration.ofMillis(101));
        var fresh = new SubmitQueryResponse("q-2", "RUNNING", null, List.of());
        assertThat(registry.submit("alice", "key", REQUEST, () -> CompletableFuture.completedFuture(fresh))
                .toCompletableFuture()).isCompletedWithValue(fresh);
    }

    @Test
    void rejectedBeforeRegistrationCanRetryButRecordedFailureIsReplayed() {
        var registry = new IdempotencyKeyRegistry();
        var retryable = new IllegalStateException("temporarily unavailable");
        assertThatThrownBy(() -> registry.submit("alice", "key", REQUEST,
                () -> CompletableFuture.failedFuture(retryable)).toCompletableFuture().join())
                .hasCause(retryable);
        var failure = new RecordedSubmissionException("q-1",
                new IllegalStateException("already registered"));
        assertThatThrownBy(() -> registry.submit("alice", "key", REQUEST, () -> { throw failure; })
                .toCompletableFuture().join()).hasCause(failure);
        assertThatThrownBy(() -> registry.submit("alice", "key", REQUEST,
                () -> CompletableFuture.completedFuture(RECEIPT)).toCompletableFuture().join()).hasCause(failure);
    }

    @Test
    void drainingAnExpiredReceiptPreservesItsSameKeyReplacement() {
        var clock = new TestClock(Instant.EPOCH);
        var registry = new IdempotencyKeyRegistry(Duration.ofMillis(100), clock);
        for (int index = 0; index < 32; index++) {
            registry.submit("alice", "prior-" + index, REQUEST, () -> CompletableFuture.completedFuture(RECEIPT));
        }
        clock.advance(Duration.ofMillis(1));
        registry.submit("alice", "reused", REQUEST, () -> CompletableFuture.completedFuture(RECEIPT));
        clock.advance(Duration.ofMillis(100));
        var replacement = new SubmitQueryResponse("q-replacement", "RUNNING", null, List.of());
        assertThat(registry.submit("alice", "reused", REQUEST, () -> CompletableFuture.completedFuture(replacement))
                .toCompletableFuture()).isCompletedWithValue(replacement);

        registry.submit("alice", "fresh-1", REQUEST, () -> CompletableFuture.completedFuture(RECEIPT));
        registry.submit("alice", "fresh-2", REQUEST, () -> CompletableFuture.completedFuture(RECEIPT));
        assertThat(registry.submit("alice", "reused", REQUEST, () -> {
            throw new AssertionError("expired receipt removed its replacement");
        }).toCompletableFuture()).isCompletedWithValue(replacement);
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
