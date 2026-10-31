package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.Objects;

import org.pangrova.shuffle.api.ShuffleCapabilities;

/**
 * Enforces the ADR-002 D3 retry-policy binding: task-level retry (fault-tolerant execution) may only
 * run on a shuffle provider whose committed partitions survive task failure.
 *
 * <p>When task-level retry is requested on an {@code EPHEMERAL} provider (today's native Arrow
 * Flight), this fails fast at startup rather than silently degrading to whole-query retry. Silent
 * degradation is the trap ADR-002 D3 forbids: operators would believe FTE is active when a single
 * worker loss still triggers a full recomputation.
 *
 * <p>The decision is bound to {@link ShuffleCapabilities#supportsTaskRetry()} — the single source of
 * truth — never to the provider's name.
 */
public final class ShuffleRetryPolicyValidator {

    private ShuffleRetryPolicyValidator() {
    }

    /**
     * Validates that the resolved provider can back the requested retry policy.
     *
     * @param taskRetryRequested whether the configured retry policy demands task-level retry (FTE)
     * @param capabilities       the resolved provider's declared capabilities
     * @throws NullPointerException  if {@code capabilities} is null
     * @throws IllegalStateException if task retry is requested but the provider does not support it
     */
    public static void validate(boolean taskRetryRequested, ShuffleCapabilities capabilities) {
        Objects.requireNonNull(capabilities, "capabilities must not be null");
        if (taskRetryRequested && !capabilities.supportsTaskRetry()) {
            throw new IllegalStateException(
                    "Task-level retry (fault-tolerant execution) requires a shuffle provider whose "
                            + "committed partitions survive task failure, but the configured provider "
                            + "is " + capabilities.durability() + " (no task retry). Select a "
                            + "LOCAL_SPILL or DURABLE provider "
                            + "(shuffle.manager=local-spill|object-store) or disable "
                            + "task-level retry.");
        }
    }
}
