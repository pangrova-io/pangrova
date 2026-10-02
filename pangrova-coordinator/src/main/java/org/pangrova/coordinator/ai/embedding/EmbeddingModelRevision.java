package org.pangrova.coordinator.ai.embedding;

import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;

import java.util.Objects;

/** Builds and validates one fully deployment-bound embedding catalog revision. */
public final class EmbeddingModelRevision {

    private EmbeddingModelRevision() {}

    public static EmbeddingModelDescriptor applyUpdate(
            EmbeddingModelDescriptor current, ModelUpdateRequest update) {
        Objects.requireNonNull(current, "current must not be null");
        Objects.requireNonNull(update, "update must not be null");
        if (!update.hasUpdates()) {
            throw new IllegalArgumentException("No fields to update in ALTER request");
        }
        return new EmbeddingModelDescriptor(
                current.name(),
                new EmbeddingModelDescriptor.ProviderRoute(
                        current.provider(),
                        update.endpoint().orElse(current.endpoint()),
                        update.apiKeySecret().orElse(current.apiKeySecret())),
                new EmbeddingModelDescriptor.EmbeddingContract(
                        current.dimension(),
                        update.queryPrefix().orElse(current.queryPrefix()),
                        update.documentPrefix().orElse(current.documentPrefix())),
                new EmbeddingModelDescriptor.ServingLimits(
                        update.maxBatchSize().orElse(current.maxBatchSize()),
                        update.maxRequestsPerSecond().orElse(
                                current.maxRequestsPerSecond())),
                Math.addExact(current.version(), 1),
                EmbeddingModelDescriptor.DeploymentFence.unbound());
    }

    static void requireValidReplacement(
            String name,
            EmbeddingModelDescriptor current,
            EmbeddingModelDescriptor replacement,
            int expectedVersion) {
        Objects.requireNonNull(replacement, "replacement must not be null");
        if (current.version() != expectedVersion) {
            throw new VersionConflictException(name, expectedVersion, current.version());
        }
        if (!name.equals(replacement.name())
                || replacement.version() != Math.addExact(expectedVersion, 1)) {
            throw new IllegalArgumentException(
                    "Replacement name/version does not match ALTER target");
        }
        if (!current.provider().equals(replacement.provider())
                || current.dimension() != replacement.dimension()) {
            throw new IllegalArgumentException(
                    "ALTER cannot change provider or embedding dimension");
        }
        if (!replacement.isDeploymentBound()) {
            throw new IllegalArgumentException(
                    "ALTER replacement must be bound to an exact Worker deployment");
        }
    }
}
