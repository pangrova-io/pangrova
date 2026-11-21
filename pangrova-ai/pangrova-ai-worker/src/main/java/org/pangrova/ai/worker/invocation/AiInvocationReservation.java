package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.worker.context.BoundAiInvocation;

import static org.pangrova.ai.worker.invocation.AiInvocationBudgetManager.budgetExceeded;

/** Logical work reservation; physical transport attempts are admitted separately. */
record AiInvocationReservation(int items, long maxAttempts, long mediaBytes) {
    static AiInvocationReservation create(
            AiCapabilityDescriptor capability, BoundAiInvocation binding,
            int items, int providerRequests, int concurrentCalls, long mediaBytes) {
        if (items <= 0 || providerRequests <= 0 || providerRequests > items
                || concurrentCalls <= 0 || concurrentCalls > providerRequests || mediaBytes < 0) {
            throw new IllegalArgumentException("AI provider reservation is invalid");
        }
        var limits = capability.resourceLimits();
        if (!binding.requestConstraints().equals(limits.requestConstraints())) {
            throw budgetExceeded("AI request constraints differ from the exact model capability");
        }
        if (concurrentCalls > limits.maxConcurrency()
                || concurrentCalls > binding.maxProviderConcurrency()
                || items > limits.maxProviderItemsPerTask() || items > binding.maxProviderItems()
                || mediaBytes > limits.maxMediaBytes() || mediaBytes > binding.maxMediaBytes()) {
            throw budgetExceeded("AI invocation exceeds its exact model or operator envelope");
        }
        long maxAttempts = Math.multiplyExact((long) providerRequests, capability.retryPolicy().maxAttempts());
        return new AiInvocationReservation(items, maxAttempts, mediaBytes);
    }
}
