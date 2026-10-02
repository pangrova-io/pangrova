package org.pangrova.spi.ai.embedding;

import org.pangrova.spi.annotation.Experimental;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Ordered vectors and actual provider metadata for one embedding request. */
@Experimental
public record EmbeddingBatchResult(
        List<float[]> vectors,
        String providerRequestId) {

    public EmbeddingBatchResult {
        vectors = copy(vectors);
        providerRequestId = Objects.requireNonNull(
                providerRequestId, "providerRequestId must not be null");
    }

    @Override
    public List<float[]> vectors() {
        return copy(vectors);
    }

    private static List<float[]> copy(List<float[]> source) {
        Objects.requireNonNull(source, "vectors must not be null");
        List<float[]> copied = new ArrayList<>(source.size());
        for (float[] vector : source) {
            copied.add(vector == null ? null : vector.clone());
        }
        return Collections.unmodifiableList(copied);
    }
}
