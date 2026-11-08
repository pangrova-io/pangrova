package org.pangrova.ai.spi.invocation;

import org.pangrova.ai.api.error.AiInvocationException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Internal context for one physical upstream provider request. Built-in
 * transports use its stable key for every retry of the same request.
 */
public final class AiProviderCallContext {

    private static final ThreadLocal<Call> CURRENT =
            new ThreadLocal<>();

    private AiProviderCallContext() {}

    /** Returns the idempotency key bound to the current provider call, if any. */
    public static Optional<String> currentIdempotencyKey() {
        return Optional.ofNullable(CURRENT.get()).map(Call::idempotencyKey);
    }

    /** Returns the absolute task deadline bound to the current provider call. */
    public static Optional<Instant> currentDeadline() {
        return Optional.ofNullable(CURRENT.get()).map(Call::deadline);
    }

    /** Carries physical-request admission into provider-owned batch threads. */
    public static Optional<AttemptAdmission> currentAttemptAdmission() {
        return Optional.ofNullable(CURRENT.get()).map(Call::attemptAdmission);
    }

    /** Reserves one complete transport send, retaining permits through its response. */
    public static PhysicalAttempt openAttempt(long encodedRequestBytes) {
        return openAttempt(encodedRequestBytes, Instant.now());
    }

    static PhysicalAttempt openAttempt(long encodedRequestBytes, Instant now) {
        Objects.requireNonNull(now, "now must not be null");
        Call call = CURRENT.get();
        if (call == null || call.attemptAdmission() == null) {
            throw new AiInvocationException(AiInvocationException.Category.AUTHORIZATION, false,
                    "AI provider transport has no governed physical-request admission");
        }
        if (!now.isBefore(call.deadline())) {
            throw deadlineExceeded();
        }
        return call.attemptAdmission().open(encodedRequestBytes);
    }

    /** Caps a configured transport wait to the current task's remaining lifetime. */
    public static Duration remainingTimeout(Duration configuredTimeout) {
        Objects.requireNonNull(configuredTimeout, "configuredTimeout must not be null");
        if (configuredTimeout.isZero() || configuredTimeout.isNegative()) {
            throw new IllegalArgumentException("configuredTimeout must be positive");
        }
        Call call = CURRENT.get();
        if (call == null) {
            return configuredTimeout;
        }
        Duration remaining = Duration.between(Instant.now(), call.deadline());
        if (remaining.isZero() || remaining.isNegative()) {
            throw deadlineExceeded();
        }
        return remaining.compareTo(configuredTimeout) < 0 ? remaining : configuredTimeout;
    }

    /** Caps a retry/rate-limit delay and fails before sleeping past the task deadline. */
    public static long remainingWaitMillis(long requestedMillis) {
        if (requestedMillis <= 0) {
            throw new IllegalArgumentException("requestedMillis must be positive");
        }
        Duration remaining = remainingTimeout(Duration.ofMillis(requestedMillis));
        return Math.max(1L, remaining.toMillis());
    }

    /** Runs the first physical provider request under one logical invocation key. */
    public static <T> T callWith(
            String invocationIdempotencyKey,
            Instant deadline,
            Supplier<T> callback) {
        return callWithOrdinal(invocationIdempotencyKey, deadline, 0, callback);
    }

    /** Runs one governed call with per-send task/operator admission. */
    public static <T> T callWith(
            String invocationIdempotencyKey,
            Instant deadline,
            AttemptAdmission attemptAdmission,
            Supplier<T> callback) {
        return callWithOrdinal(
                invocationIdempotencyKey, deadline, 0, attemptAdmission, callback);
    }

    /** Runs one numbered physical provider request under a stable derived key. */
    public static <T> T callWithOrdinal(
            String invocationIdempotencyKey,
            Instant deadline,
            int ordinal,
            Supplier<T> callback) {
        Call current = CURRENT.get();
        return callWithOrdinal(invocationIdempotencyKey, deadline, ordinal,
                current == null ? null : current.attemptAdmission(), callback);
    }

    /** Propagates the same admission into a numbered provider batch request. */
    public static <T> T callWithOrdinal(
            String invocationIdempotencyKey,
            Instant deadline,
            int ordinal,
            AttemptAdmission attemptAdmission,
            Supplier<T> callback) {
        if (invocationIdempotencyKey == null
                || invocationIdempotencyKey.isBlank()) {
            throw new IllegalArgumentException(
                    "provider invocation idempotency key must not be blank");
        }
        Objects.requireNonNull(deadline, "provider deadline must not be null");
        if (!Instant.now().isBefore(deadline)) {
            throw deadlineExceeded();
        }
        if (ordinal < 0) {
            throw new IllegalArgumentException(
                    "provider request ordinal must not be negative");
        }
        Objects.requireNonNull(callback, "callback must not be null");
        Call previous = CURRENT.get();
        CURRENT.set(new Call(
                invocationIdempotencyKey + ":request-" + ordinal,
                deadline, attemptAdmission));
        try {
            return callback.get();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    private static AiInvocationException deadlineExceeded() {
        return new AiInvocationException(
                AiInvocationException.Category.TIMEOUT,
                false,
                "AI provider call exceeded the task deadline");
    }

    private record Call(
            String idempotencyKey, Instant deadline, AttemptAdmission attemptAdmission) {}

}
