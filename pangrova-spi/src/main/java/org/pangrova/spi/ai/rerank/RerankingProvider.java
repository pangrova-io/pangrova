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
package org.pangrova.spi.ai.rerank;

import org.pangrova.spi.ai.AiCapabilityDescriptor;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.ai.Modality;
import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.annotation.ThreadSafe;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/** Provider boundary for version-exact query-document relevance scoring. */
@Experimental
@ThreadSafe
public interface RerankingProvider {

    /** Default initial delay for retryable provider failures. */
    Duration DEFAULT_RETRY_INITIAL_DELAY = Duration.ofMillis(500);

    /**
     * Scores documents in input order. Implementations must return exactly one
     * finite score per document and must not silently omit failed items.
     */
    RerankingBatchResult score(String query, List<String> documents);

    String modelName();

    default String modelVersion() {
        return "unversioned";
    }

    /** Exact public model descriptor signature, or an empty array if unavailable. */
    default byte[] modelSignatureSha256() {
        return new byte[0];
    }

    int maxBatchSize();

    /** Exact serving limits for this provider deployment. */
    default AiCapabilityDescriptor.ResourceLimits resourceLimits() {
        throw new IllegalStateException(
                "Reranking provider must declare exact resource limits");
    }

    /** Returns the canonical capability descriptor advertised by this provider. */
    default Set<AiCapabilityDescriptor> capabilities() {
        return Set.of(new AiCapabilityDescriptor(
                "reranking:" + modelName(),
                modelVersion(),
                AiCapabilityDescriptor.Operation.RERANKING,
                List.of(new AiCapabilityDescriptor.Signature(
                        "DOUBLE", List.of("VARCHAR", "VARCHAR"))),
                Set.of(
                        AiCapabilityDescriptor.ExecutionMode.SCALAR,
                        AiCapabilityDescriptor.ExecutionMode.BATCH,
                        AiCapabilityDescriptor.ExecutionMode.SYNC),
                Set.of(Modality.TEXT),
                maxBatchSize(),
                resourceLimits(),
                false,
                new AiCapabilityDescriptor.RetryPolicy(
                        4,
                        DEFAULT_RETRY_INITIAL_DELAY,
                        Duration.ofSeconds(4),
                        Set.of(
                                AiInvocationException.Category.RATE_LIMIT,
                                AiInvocationException.Category.TIMEOUT,
                                AiInvocationException.Category.PROVIDER_UNAVAILABLE)),
                Set.of("AI_MODEL_USAGE")));
    }
}
