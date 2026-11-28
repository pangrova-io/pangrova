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
package org.pangrova.ai.worker.provider.governance;

import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.AiInvocationResult;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.RerankingBatchResult;
import org.pangrova.ai.api.reranking.RerankingProvider;
import org.pangrova.ai.worker.invocation.AiInvocationService;

import java.util.List;
import java.util.Set;

/** Reranking facade that enforces the canonical governed invocation path. */
public final class GovernedRerankingProvider implements RerankingProvider {

    private final RerankingProvider delegate;
    private final AiInvocationService service;

    /** Wraps a provider so every reranking call uses the governed invocation service. */
    public GovernedRerankingProvider(
            RerankingProvider delegate, AiInvocationService service) {
        this.delegate = java.util.Objects.requireNonNull(
                delegate, "delegate must not be null");
        this.service = java.util.Objects.requireNonNull(
                service, "service must not be null");
    }

    @Override
    public RerankingBatchResult score(String query, List<String> documents) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("reranking query must not be blank");
        }
        if (documents == null || documents.isEmpty()) {
            throw new IllegalArgumentException(
                    "reranking documents must not be empty");
        }
        AiCapabilityDescriptor capability = AiInvocationRequests.requireCapability(
                capabilities(), AiOperation.RERANKING);
        List<AiInvocationRequest.Input> inputs = documents.stream()
                .map(document -> (AiInvocationRequest.Input)
                        new AiInvocationRequest.RerankInput(query, document))
                .toList();
        var request = AiInvocationRequests.create(
                modelName(),
                modelVersion(),
                modelSignatureSha256(),
                capability,
                documents.size() == 1
                        ? AiExecutionMode.SCALAR
                        : AiExecutionMode.BATCH,
                inputs,
                GenerationParameters.DEFAULTS);
        AiInvocationResult result = service.invoke(request);
        List<Double> scores = result.outputs().stream()
                .map(GovernedRerankingProvider::score)
                .toList();
        return new RerankingBatchResult(
                scores, result.providerRequestId());
    }

    private static double score(AiInvocationResult.Output output) {
        if (output instanceof AiInvocationResult.ScoreOutput score) {
            return score.score();
        }
        throw new IllegalStateException(
                "Reranking invocation returned a non-score output");
    }

    @Override public String modelName() { return delegate.modelName(); }
    @Override public String modelVersion() { return delegate.modelVersion(); }
    @Override public byte[] modelSignatureSha256() {
        return delegate.modelSignatureSha256();
    }
    @Override public int maxBatchSize() { return delegate.maxBatchSize(); }
    @Override public Set<AiCapabilityDescriptor> capabilities() {
        return delegate.capabilities();
    }
}
