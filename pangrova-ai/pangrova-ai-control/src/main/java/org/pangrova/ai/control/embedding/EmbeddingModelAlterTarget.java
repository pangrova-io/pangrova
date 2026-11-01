package org.pangrova.ai.control.embedding;

import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;

import java.util.Objects;

/** Immutable catalog identity captured before resolving an ALTER deployment. */
public record EmbeddingModelAlterTarget(
        EmbeddingModelDescriptor descriptor,
        String incarnationId) {

    public EmbeddingModelAlterTarget {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        if (incarnationId == null || incarnationId.isBlank()) {
            throw new IllegalArgumentException("incarnationId must not be blank");
        }
    }
}
