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
package org.pangrova.ai.api.embedding;

import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.enums.EmbeddingPurpose;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.concurrency.ThreadSafe;

/**
 * SPI contract for embedding model providers.
 *
 * <p>Implementations produce dense vector embeddings from text or binary content.
 * The engine invokes this interface during DML execution (SYNC mode) and
 * background drain (ASYNC mode). Worker runtime configuration selects and registers the provider implementation.</p>
 */
@Experimental
@ThreadSafe
public interface EmbeddingProvider {

    /**
     * Embeds one ordered batch using its explicit semantic purpose.
     *
     * <p>One input and many inputs have the same semantics. Providers must return
     * one vector per input in the same order, or fail the complete request.</p>
     *
     * @param request nonempty homogeneous input, size at most {@link #maxBatchSize()}
     * @return complete ordered vectors and provider request identity
     * @throws IllegalArgumentException if the request exceeds the serving limits
     * @throws AiInvocationException if purpose or modality is not supported
     */
    EmbeddingBatchResult embed(EmbeddingBatchRequest request);

    /** Exact purposes supported by this provider, checked before any provider I/O. */
    Set<EmbeddingPurpose> supportedPurposes();

    /**
     * Returns the output vector dimensionality of this provider.
     *
     * @return positive integer representing the embedding dimension
     */
    int dimensions();

    /**
     * Returns the model name this provider serves.
     *
     * @return the model name (never null or blank)
     */
    String modelName();

    /**
     * Returns the maximum number of inputs per batch request.
     *
     * @return positive integer representing the max batch size
     */
    int maxBatchSize();

    /** Exact provider-visible model version used in invocation/audit records. */
    default String modelVersion() {
        return "unversioned";
    }

    /** Exact public model descriptor signature, or an empty array if unavailable. */
    default byte[] modelSignatureSha256() {
        return new byte[0];
    }

    /** Exact serving limits for this provider deployment. */
    default AiCapabilityDescriptor.ResourceLimits resourceLimits() {
        throw new IllegalStateException(
                "Embedding provider must declare exact resource limits");
    }

    /** Canonical capability descriptor consumed by dispatch and governance. */
    default Set<AiCapabilityDescriptor> capabilities() {
        Set<Modality> modalities = Set.copyOf(supportedModalities());
        List<AiCapabilityDescriptor.Signature> signatures = new ArrayList<>();
        signatures.add(new AiCapabilityDescriptor.Signature(
                "VECTOR", List.of("VARCHAR")));
        if (modalities.stream().anyMatch(modality -> modality != Modality.TEXT)) {
            signatures.add(new AiCapabilityDescriptor.Signature(
                    "VECTOR", List.of("FILE")));
        }
        return Set.of(new AiCapabilityDescriptor(
                "embedding:" + modelName(),
                modelVersion(),
                AiOperation.EMBEDDING,
                signatures,
                Set.of(AiExecutionMode.SCALAR,
                        AiExecutionMode.BATCH,
                        AiExecutionMode.SYNC,
                        AiExecutionMode.ASYNC),
                modalities,
                maxBatchSize(),
                resourceLimits(),
                true,
                AiCapabilityDescriptor.RetryPolicy.noRetry(),
                modalities.stream().anyMatch(modality -> modality != Modality.TEXT)
                        ? Set.of("AI_MODEL_USAGE", "AI_MEDIA_EGRESS")
                        : Set.of("AI_MODEL_USAGE")));
    }

    /**
     * Modalities accepted by this exact model. Text is the conservative
     * default; providers must explicitly advertise binary modalities.
     */
    default Set<Modality> supportedModalities() {
        return Set.of(Modality.TEXT);
    }
}
