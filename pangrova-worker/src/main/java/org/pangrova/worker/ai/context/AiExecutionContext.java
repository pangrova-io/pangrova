package org.pangrova.worker.ai.context;

import org.pangrova.protocol.v2.TaskAttemptId;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Trusted per-task identity made available while a JVM-backed AI SQL function
 * executes. The native engine derives it from the internally authenticated
 * coordinator task request and scopes it to the current callback thread.
 */
public record AiExecutionContext(
        String tenantId,
        String principal,
        String queryId,
        String taskId,
        String traceId,
        long deadlineEpochMs,
        int maxProviderConcurrency,
        long maxProviderItems,
        long maxProviderAttempts,
        long maxProviderRequestBytes,
        long maxProviderResponseBytes,
        long maxMediaBytes,
        TaskAttemptId taskAttempt) {

    private static final ThreadLocal<AiExecutionContext> CURRENT = new ThreadLocal<>();
    private static final ThreadLocal<String> PROVIDER_IDEMPOTENCY_BASE =
            new ThreadLocal<>();
    private static final ThreadLocal<BoundAiInvocation> BOUND_INVOCATION =
            new ThreadLocal<>();

    public AiExecutionContext {
        tenantId = normalize(tenantId);
        principal = normalize(principal);
        queryId = normalize(queryId);
        taskId = normalize(taskId);
        traceId = normalize(traceId);
        Objects.requireNonNull(taskAttempt, "canonical taskAttempt required");
        if (!taskAttempt.getQueryOrJobId().equals(queryId) || !taskAttempt.getTaskId().equals(taskId)) {
            throw new IllegalArgumentException("AI context identity differs from its canonical task attempt");
        }
        if (deadlineEpochMs < 0) {
            throw new IllegalArgumentException("deadlineEpochMs must not be negative");
        }
        if (maxProviderConcurrency < 0 || maxProviderItems < 0 || maxProviderAttempts < 0
                || maxProviderRequestBytes < 0 || maxProviderResponseBytes < 0 || maxMediaBytes < 0) {
            throw new IllegalArgumentException(
                    "AI execution resource grants must not be negative");
        }
    }

    /** Fails closed when a provider-backed operator has no bounded grant. */
    public void requireProviderBudget() {
        if (maxProviderConcurrency <= 0 || maxProviderItems <= 0 || maxProviderAttempts <= 0
                || maxProviderRequestBytes <= 0 || maxProviderResponseBytes <= 0) {
            throw new IllegalStateException(
                    "AI execution provider resource grant is not available");
        }
    }

    /** Returns the execution context bound to the current callback thread. */
    public static Optional<AiExecutionContext> current() {
        return Optional.ofNullable(CURRENT.get());
    }

    /** Decodes the sole canonical JNI callback authority; there is no positional ABI. */
    public static AiExecutionContext fromCanonicalCallback(byte[] bytes) {
        return CanonicalAiExecutionContexts.decode(bytes);
    }

    /** Same canonical projection used when registering the actual Worker task authority. */
    public static AiExecutionContext fromTaskRequest(org.pangrova.protocol.v2.TaskRequest request) {
        return fromCanonicalCallback(org.pangrova.protocol.CanonicalTaskRequests.callbackFor(request).toByteArray());
    }

    /**
     * Runs one callback with the supplied context and restores any enclosing
     * context even when the callback fails.
     */
    public static <T> T callWith(AiExecutionContext context, Supplier<T> callback) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(callback, "callback must not be null");
        AiExecutionContext previous = CURRENT.get();
        CURRENT.set(context);
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

    /**
     * Binds the stable operator idempotency base while a provider-backed
     * Native expression is evaluated. The value originates from the operator
     * execution policy, not from a user-supplied SQL value.
     */
    public static <T> T callWithProviderIdempotencyBase(
            String idempotencyBase, Supplier<T> callback) {
        if (idempotencyBase == null || idempotencyBase.isBlank()) {
            throw new IllegalArgumentException(
                    "provider idempotency base must not be blank");
        }
        Objects.requireNonNull(callback, "callback must not be null");
        String previous = PROVIDER_IDEMPOTENCY_BASE.get();
        PROVIDER_IDEMPOTENCY_BASE.set(idempotencyBase);
        try {
            return callback.get();
        } finally {
            if (previous == null) {
                PROVIDER_IDEMPOTENCY_BASE.remove();
            } else {
                PROVIDER_IDEMPOTENCY_BASE.set(previous);
            }
        }
    }

    /** Stable retry identity of the currently executing model operator. */
    public static Optional<String> providerIdempotencyBase() {
        return Optional.ofNullable(PROVIDER_IDEMPOTENCY_BASE.get());
    }

    /** Runs one expression callback under its exact task OperatorBinding. */
    public static <T> T callWithBoundInvocation(
            BoundAiInvocation binding, Supplier<T> callback) {
        Objects.requireNonNull(binding, "binding must not be null");
        BoundAiInvocation previous = BOUND_INVOCATION.get();
        BOUND_INVOCATION.set(binding);
        try {
            return callWithProviderIdempotencyBase(
                    binding.idempotencyBase(), callback);
        } finally {
            if (previous == null) {
                BOUND_INVOCATION.remove();
            } else {
                BOUND_INVOCATION.set(previous);
            }
        }
    }

    /** Exact model/operator identity bound by the Native plan converter. */
    public static Optional<BoundAiInvocation> boundInvocation() {
        return Optional.ofNullable(BOUND_INVOCATION.get());
    }

    /** Returns the absolute invocation deadline; zero is represented as empty. */
    public Optional<Instant> deadline() {
        return deadlineEpochMs == 0
                ? Optional.empty()
                : Optional.of(Instant.ofEpochMilli(deadlineEpochMs));
    }

    /** Fails closed when identity required for an AI provider call is absent. */
    public void requireGovernedIdentity() {
        requireText(tenantId, "tenantId");
        requireText(principal, "principal");
        requireText(queryId, "queryId");
        requireText(taskId, "taskId");
        requireText(traceId, "traceId");
        if (deadlineEpochMs <= 0) {
            throw new IllegalStateException("AI execution deadline is not available");
        }
    }

    private static String normalize(String value) {
        return value == null ? "" : value;
    }

    private static void requireText(String value, String field) {
        if (value.isBlank()) {
            throw new IllegalStateException("AI execution " + field + " is not available");
        }
    }
}
