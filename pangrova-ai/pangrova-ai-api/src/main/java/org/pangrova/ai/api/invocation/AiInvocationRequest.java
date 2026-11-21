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
package org.pangrova.ai.api.invocation;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.enums.EmbeddingPurpose;
import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.pangrova.common.util.Preconditions.requireText;

/** Immutable request crossing the engine/provider AI invocation boundary. */
@Experimental
public record AiInvocationRequest(
        String requestId,
        String tenantId,
        String principal,
        String queryId,
        String traceId,
        String modelName,
        String modelVersion,
        byte[] modelSignatureSha256,
        String capabilityVersion,
        AiOperation operation,
        AiExecutionMode executionMode,
        Optional<EmbeddingPurpose> embeddingPurpose,
        List<Input> inputs,
        GenerationParameters generationParameters,
        Instant deadline,
        String idempotencyKey,
        Map<String, String> attributes) {

    public sealed interface Input permits
            TextInput, BlobInput, MultimodalInput, VectorInput, RerankInput {}

    public record TextInput(String text) implements Input {
        public TextInput {
            Objects.requireNonNull(text, "text must not be null");
        }
    }

    public record BlobInput(BlobReference reference) implements Input {
        public BlobInput {
            Objects.requireNonNull(reference, "reference must not be null");
        }
    }

    public record MultimodalInput(MultimodalPrompt prompt) implements Input {
        public MultimodalInput {
            Objects.requireNonNull(prompt, "prompt must not be null");
        }
    }

    public record VectorInput(float[] vector) implements Input {
        public VectorInput {
            if (vector == null || vector.length == 0) {
                throw new IllegalArgumentException("vector must not be null or empty");
            }
            vector = vector.clone();
        }

        @Override
        public float[] vector() {
            return vector.clone();
        }
    }

    /** One query-document pair in an ordered reranking batch. */
    public record RerankInput(String query, String document) implements Input {
        public RerankInput {
            requireText(query, "query");
            Objects.requireNonNull(document, "document must not be null");
        }
    }

    public AiInvocationRequest {
        requireText(requestId, "requestId");
        requireText(tenantId, "tenantId");
        requireText(principal, "principal");
        requireText(queryId, "queryId");
        requireText(traceId, "traceId");
        requireText(modelName, "modelName");
        requireText(modelVersion, "modelVersion");
        modelSignatureSha256 = Objects.requireNonNull(
                modelSignatureSha256, "modelSignatureSha256 must not be null").clone();
        if (modelSignatureSha256.length != 32) {
            throw new IllegalArgumentException(
                    "modelSignatureSha256 must contain exactly 32 bytes");
        }
        requireText(capabilityVersion, "capabilityVersion");
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(executionMode, "executionMode must not be null");
        embeddingPurpose = Objects.requireNonNull(embeddingPurpose, "embeddingPurpose required");
        if ((operation == AiOperation.EMBEDDING) != embeddingPurpose.isPresent()) {
            throw new IllegalArgumentException("only embedding requests require an explicit purpose");
        }
        inputs = List.copyOf(Objects.requireNonNull(inputs, "inputs must not be null"));
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("inputs must not be empty");
        }
        generationParameters = generationParameters == null
                ? GenerationParameters.DEFAULTS
                : generationParameters;
        Objects.requireNonNull(deadline, "deadline must not be null");
        requireText(idempotencyKey, "idempotencyKey");
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    @Override
    public byte[] modelSignatureSha256() {
        return modelSignatureSha256.clone();
    }

    /**
     * Verifies that this request can still be dispatched at the supplied time.
     *
     * @param now current time
     * @throws AiInvocationException if the request deadline has elapsed
     */
    public void requireNotExpired(Instant now) {
        if (!deadline.isAfter(now)) {
            throw new AiInvocationException(
                    AiFailureCategory.TIMEOUT, false,
                    "AI invocation deadline has expired for request " + requestId);
        }
    }
}
