package org.pangrova.generation.api.exchange;

import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;

import java.util.Objects;

/** Immutable external allocation identity. This contract deliberately contains no access capabilities. */
public record GenerationExchangeRegistration(
        String exchangeId, String executionId, String shuffleId, int producerStageId,
        long planEpoch, String graphSha256, String stagePlanSha256, String sourceSnapshot,
        int producerCount, String producerSetSha256, int partitionCount,
        ShuffleProviderDescriptor descriptor, GenerationExecutionOwner registeredOwner) {

    public GenerationExchangeRegistration {
        requireText(exchangeId);
        requireText(executionId);
        requireText(shuffleId);
        requireText(sourceSnapshot);
        requireSha256(graphSha256);
        requireSha256(stagePlanSha256);
        requireSha256(producerSetSha256);
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(registeredOwner, "registeredOwner");
        if (producerStageId < 0 || planEpoch <= 0 || producerCount <= 0 || partitionCount <= 0) {
            throw new IllegalArgumentException("Invalid durable exchange cohort");
        }
    }

    static void requireText(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Exchange identity is required");
    }

    static void requireSha256(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Canonical exchange SHA-256 is required");
        }
    }
}
