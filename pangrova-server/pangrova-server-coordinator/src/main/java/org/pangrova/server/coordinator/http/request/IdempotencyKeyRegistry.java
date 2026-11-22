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
import jakarta.ws.rs.core.Response;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.util.DigestUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Coordinates authenticated HTTP submissions on one Coordinator, including concurrent retries. */
public final class IdempotencyKeyRegistry {
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(10);
    private static final int MAX_EXPIRATIONS_PER_SUBMISSION = 16;

    private record Key(String principal, String idempotencyKey) {}

    private static final class Entry {
        final String fingerprint;
        final CompletableFuture<SubmitQueryResponse> receipt = new CompletableFuture<>();
        volatile long expiresAtMillis = Long.MAX_VALUE;

        Entry(String fingerprint) {
            this.fingerprint = fingerprint;
        }
    }

    private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();
    private final DelayQueue<Expiration> expirations = new DelayQueue<>();
    private final long ttlMillis;
    private final Clock clock;

    private final class Expiration implements Delayed {
        private final Key key;
        private final Entry entry;

        private Expiration(Key key, Entry entry) {
            this.key = key;
            this.entry = entry;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(entry.expiresAtMillis - clock.millis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(entry.expiresAtMillis, ((Expiration) other).entry.expiresAtMillis);
        }
    }

    public IdempotencyKeyRegistry() {
        this(DEFAULT_TTL, Clock.systemUTC());
    }

    IdempotencyKeyRegistry(Duration ttl, Clock clock) {
        this.ttlMillis = ttl.toMillis();
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("Idempotency TTL must be positive");
        }
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Shares the original successful receipt only with a matching principal and request. A
     * concurrent retry joins the in-flight submission. The TTL starts on success; a rejected
     * pre-registration submission is released for a later retry. Recorded failures keep their query
     * identity and cannot rerun the SQL during the TTL. The request fingerprint is fixed-size.
     */
    public SubmitQueryResponse submit(String principal, String key, SubmitQueryRequest request,
            Supplier<SubmitQueryResponse> submit) {
        var scopedKey = new Key(Objects.requireNonNull(principal, "principal"), Objects.requireNonNull(key, "key"));
        long now = clock.millis();
        retireExpiredReceipts();
        var candidate = new Entry(fingerprint(request));
        Entry selected = entries.compute(scopedKey, (ignored, existing) ->
                existing == null || existing.expiresAtMillis <= now ? candidate : existing);
        if (!selected.fingerprint.equals(candidate.fingerprint)) {
            throw new ClientErrorException("Idempotency key was already used for a different request",
                    Response.Status.CONFLICT);
        }
        if (selected != candidate) {
            try {
                return selected.receipt.join();
            } catch (CompletionException failure) {
                if (failure.getCause() instanceof RuntimeException cause) {
                    throw cause;
                }
                if (failure.getCause() instanceof Error cause) {
                    throw cause;
                }
                throw failure;
            }
        }
        try {
            SubmitQueryResponse receipt = Objects.requireNonNull(submit.get(), "submission receipt");
            selected.receipt.complete(receipt);
            retainReceipt(scopedKey, selected);
            return receipt;
        } catch (RuntimeException | Error failure) {
            selected.receipt.completeExceptionally(failure);
            if (failure instanceof RecordedSubmissionException || failure instanceof Error) {
                retainReceipt(scopedKey, selected);
            } else {
                entries.remove(scopedKey, selected);
            }
            throw failure;
        }
    }

    private void retireExpiredReceipts() {
        for (int removed = 0; removed < MAX_EXPIRATIONS_PER_SUBMISSION; removed++) {
            Expiration expired = expirations.poll();
            if (expired == null) return;
            entries.remove(expired.key, expired.entry);
        }
    }

    private void retainReceipt(Key key, Entry entry) {
        entry.expiresAtMillis = clock.millis() + ttlMillis;
        expirations.add(new Expiration(key, entry));
    }

    private static String fingerprint(SubmitQueryRequest request) {
        MessageDigest digest = DigestUtils.sha256();
        update(digest, request.query());
        new TreeMap<>(request.sessionProperties()).forEach((name, value) -> {
            update(digest, name);
            update(digest, value);
        });
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
