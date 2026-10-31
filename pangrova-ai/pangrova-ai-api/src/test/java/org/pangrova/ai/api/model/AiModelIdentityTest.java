package org.pangrova.ai.api.model;

import org.pangrova.ai.api.capability.AiRequestConstraints;
import org.pangrova.ai.api.capability.Modality;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.error.AiInvocationException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class AiModelIdentityTest {

    @Test
    void signatureFencesInvocationLimitAndRetryPolicy() {
        AiCapabilityDescriptor baseline = capability(
                32,
                new AiCapabilityDescriptor.RetryPolicy(
                        4, Duration.ofMillis(500), Duration.ofSeconds(4),
                        Set.of(AiInvocationException.Category.RATE_LIMIT)));
        AiCapabilityDescriptor invocationLimitChanged = capability(
                33, baseline.retryPolicy());
        AiCapabilityDescriptor retryChanged = capability(
                32,
                new AiCapabilityDescriptor.RetryPolicy(
                        3, Duration.ofMillis(500), Duration.ofSeconds(4),
                        Set.of(AiInvocationException.Category.RATE_LIMIT)));

        byte[] signature = AiModelIdentity.signatureSha256(model(baseline));

        assertThat(signature).isNotEqualTo(AiModelIdentity.signatureSha256(
                model(invocationLimitChanged)));
        assertThat(signature).isNotEqualTo(AiModelIdentity.signatureSha256(
                model(retryChanged)));
    }

    @Test
    void signatureIndependentlyFencesBodyLimitsAndPhysicalAttempts() {
        var baseline = capability(32, AiCapabilityDescriptor.RetryPolicy.noRetry());
        var limits = baseline.resourceLimits();
        var request = limits.requestConstraints();
        var changed = List.of(
                new AiCapabilityDescriptor.ResourceLimits(new AiRequestConstraints(
                        request.maxInputTokens(), request.maxOutputTokens(), request.maxRequestBytes() + 1,
                        request.maxResponseBytes()), 0, 2, 32, 128),
                new AiCapabilityDescriptor.ResourceLimits(new AiRequestConstraints(
                        request.maxInputTokens(), request.maxOutputTokens(), request.maxRequestBytes(),
                        request.maxResponseBytes() + 1), 0, 2, 32, 128),
                new AiCapabilityDescriptor.ResourceLimits(request, 0, 2, 32, 129));
        byte[] signature = AiModelIdentity.signatureSha256(model(baseline));
        for (var envelope : changed) {
            var altered = new AiCapabilityDescriptor(baseline.capabilityId(), baseline.version(),
                    baseline.operation(), baseline.signatures(), baseline.executionModes(), baseline.modalities(),
                    baseline.maxBatchSize(), envelope, baseline.deterministic(), baseline.retryPolicy(),
                    baseline.requiredPermissions());
            assertThat(signature).isNotEqualTo(AiModelIdentity.signatureSha256(model(altered)));
        }
    }

    private static AiCapabilityDescriptor capability(
            long maxInvocations,
            AiCapabilityDescriptor.RetryPolicy retryPolicy) {
        return new AiCapabilityDescriptor(
                "inference:model", "v1",
                AiCapabilityDescriptor.Operation.TEXT_GENERATION,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VARCHAR", List.of("VARCHAR"))),
                Set.of(AiCapabilityDescriptor.ExecutionMode.BATCH),
                Set.of(Modality.TEXT),
                8,
                new AiCapabilityDescriptor.ResourceLimits(
                        new AiRequestConstraints(1000, 500, 1048576, 4194304),
                        0, 2, maxInvocations, 128),
                false,
                retryPolicy,
                Set.of("AI_MODEL_USAGE"));
    }

    private static AiModelDescriptor model(AiCapabilityDescriptor capability) {
        return new AiModelDescriptor(
                "model", "v1", "openai-compatible",
                URI.create("https://models.example/v1/chat/completions"),
                "env://KEY", List.of(capability), true,
                Map.of("provider-model", "deployment-v1"));
    }
}
