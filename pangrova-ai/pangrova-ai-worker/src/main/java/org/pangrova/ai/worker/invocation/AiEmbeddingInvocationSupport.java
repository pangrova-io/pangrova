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
package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.worker.enums.EmbeddingInputKind;

import org.pangrova.ai.api.enums.AiFailureCategory;

import static org.pangrova.ai.worker.invocation.AiInvocationFailures.invalidInput;

import java.util.List;

import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.AiInvocationResult;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.embedding.EmbeddingBatchResult;
import org.pangrova.ai.api.embedding.EmbeddingBatchRequest;
import org.pangrova.ai.api.embedding.EmbeddingProvider;
import org.pangrova.ai.api.invocation.AiProviderCallContext;
import org.pangrova.ai.worker.embedding.EmbeddingResultValidator;

/** Embedding-specific input, provider and vector validation for an invocation. */
final class AiEmbeddingInvocationSupport {

    private AiEmbeddingInvocationSupport() {}

    record EmbeddingResult(EmbeddingBatchResult providerResult, long inputBytes) {}

    static EmbeddingInputKind embeddingInputKind(AiInvocationRequest request) {
        boolean allText = request.inputs().stream()
                .allMatch(AiInvocationRequest.TextInput.class::isInstance);
        boolean allBlob = request.inputs().stream()
                .allMatch(AiInvocationRequest.BlobInput.class::isInstance);
        if (allText) {
            return EmbeddingInputKind.TEXT;
        }
        if (allBlob) {
            return EmbeddingInputKind.BLOB;
        }
        throw invalidInput(
                request, "EMBEDDING inputs must be uniformly text or blob references");
    }

    static long blobBytes(AiInvocationRequest request) {
        return request.inputs().stream()
                .map(AiInvocationRequest.BlobInput.class::cast)
                .map(AiInvocationRequest.BlobInput::reference)
                .mapToLong(BlobReference::size)
                .reduce(0L, Math::addExact);
    }

    static EmbeddingResult invokeEmbeddingProvider(
            AiInvocationRequest request,
            EmbeddingProvider provider,
            EmbeddingInputKind inputKind,
            AiPhysicalInvocationAdmission admission) {
        var purpose = request.embeddingPurpose().orElseThrow();
        if (!provider.supportedPurposes().contains(purpose)) {
            throw new AiInvocationException(AiFailureCategory.CAPABILITY_MISMATCH,
                    false, "Exact embedding provider does not support purpose " + purpose);
        }
        List<EmbeddingBatchRequest.Input> inputs = request.inputs().stream()
                .map(AiEmbeddingInvocationSupport::embeddingInput).toList();
        var batch = new EmbeddingBatchRequest(purpose, inputs);
        long inputBytes = inputKind == EmbeddingInputKind.TEXT
                ? inputs.stream().map(EmbeddingBatchRequest.TextInput.class::cast)
                        .mapToLong(input -> AiInvocationResultSupport.utf8Length(input.text()))
                        .reduce(0L, Math::addExact)
                : blobBytes(request);
        var providerResult = AiProviderCallContext.callWith(
                request.idempotencyKey(), request.deadline(),
                admission::openAttempt,
                () -> provider.embed(batch));
        return new EmbeddingResult(requireEmbeddingResult(providerResult), inputBytes);
    }

    private static EmbeddingBatchRequest.Input embeddingInput(AiInvocationRequest.Input input) {
        if (input instanceof AiInvocationRequest.TextInput text) {
            return new EmbeddingBatchRequest.TextInput(text.text());
        }
        return new EmbeddingBatchRequest.BlobInput(((AiInvocationRequest.BlobInput) input).reference());
    }

    static EmbeddingBatchResult requireEmbeddingResult(
            EmbeddingBatchResult providerResult) {
        if (providerResult == null) {
            throw new AiInvocationException(
                    AiFailureCategory.PROVIDER_RESPONSE_INVALID,
                    false,
                    "Embedding provider returned no result");
        }
        return providerResult;
    }

    static List<AiInvocationResult.Output> embeddingOutputs(
            AiInvocationRequest request,
            EmbeddingProvider provider,
            List<float[]> vectors) {
        return EmbeddingResultValidator.validateBatch(
                        provider, vectors, request.inputs().size(),
                        "AI invocation " + request.requestId())
                .stream()
                .map(vector -> (AiInvocationResult.Output)
                        new AiInvocationResult.VectorOutput(vector))
                .toList();
    }
}
