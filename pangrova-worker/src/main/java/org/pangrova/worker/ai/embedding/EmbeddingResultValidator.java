/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.worker.ai.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;

import java.util.ArrayList;
import java.util.List;

/** Enforces cardinality, order, dimension and ownership at the provider boundary. */
public final class EmbeddingResultValidator {

    private EmbeddingResultValidator() {}

    /** Validates and defensively returns one embedding vector. */
    public static float[] validateOne(
            EmbeddingProvider provider, float[] vector, String context) {
        int dimension = requireProviderContract(provider, context);
        return validateVector(provider, vector, dimension, 0, context);
    }

    /** Validates cardinality, ordering, dimension and ownership for one batch. */
    public static List<float[]> validateBatch(
            EmbeddingProvider provider,
            List<float[]> vectors,
            int expectedCount,
            String context) {
        int dimension = requireProviderContract(provider, context);
        if (vectors == null || vectors.size() != expectedCount) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_EMBEDDING_DIMENSION_MISMATCH,
                    "Embedding result cardinality mismatch for " + context
                            + " model=" + provider.modelName()
                            + " expected=" + expectedCount
                            + " actual=" + (vectors == null ? "null" : vectors.size()));
        }
        List<float[]> validated = new ArrayList<>(expectedCount);
        for (int i = 0; i < vectors.size(); i++) {
            validated.add(validateVector(provider, vectors.get(i), dimension, i, context));
        }
        return List.copyOf(validated);
    }

    /** Validates the provider's declared dimension and batch capacity. */
    public static int requireProviderContract(EmbeddingProvider provider, String context) {
        int dimension = provider.dimensions();
        if (dimension <= 0) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_EMBEDDING_DIMENSION_MISMATCH,
                    "Embedding provider reported non-positive dimensions for " + context
                            + " model=" + provider.modelName() + " dimensions=" + dimension);
        }
        if (provider.maxBatchSize() <= 0) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE,
                    "Embedding provider reported non-positive maxBatchSize for " + context
                            + " model=" + provider.modelName()
                            + " maxBatchSize=" + provider.maxBatchSize());
        }
        return dimension;
    }

    private static float[] validateVector(
            EmbeddingProvider provider,
            float[] vector,
            int dimension,
            int index,
            String context) {
        if (vector == null || vector.length != dimension) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_EMBEDDING_DIMENSION_MISMATCH,
                    "Embedding dimension mismatch for " + context
                            + " model=" + provider.modelName()
                            + " index=" + index
                            + " expected=" + dimension
                            + " actual=" + (vector == null ? "null" : vector.length));
        }
        for (int component = 0; component < vector.length; component++) {
            if (!Float.isFinite(vector[component])) {
                throw new PangrovaException(
                        PangrovaErrorCode.AI_EMBEDDING_DIMENSION_MISMATCH,
                        "Embedding contains a non-finite component for " + context
                                + " model=" + provider.modelName()
                                + " index=" + index
                                + " component=" + component);
            }
        }
        return vector.clone();
    }
}
