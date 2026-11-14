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
package org.pangrova.ai.api.inference;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.MultimodalPrompt;
import org.pangrova.ai.api.invocation.InferenceResult;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.concurrent.ThreadSafe;

import java.util.List;
import java.util.Set;

/**
 * SPI contract for generative AI / LLM inference providers.
 *
 * <p>Implementations call external model endpoints (OpenAI-compatible, vLLM,
 * Ollama, etc.) to produce text completions from prompts. The engine invokes
 * this interface on the Worker data plane during scalar and table function
 * evaluation (ai_complete, ai_summarize, ai_classify, ai_filter).</p>
 *
 * <p>Implementations must enforce local rate admission and request deadlines.
 * Model POST replay requires a separately verified provider idempotency contract;
 * the default capability admits one physical attempt.</p>
 */
@Experimental
@ThreadSafe
public interface InferenceProvider {

    /**
     * Generates a text completion for the given prompt.
     *
     * @param prompt     the prompt text (must not be null)
     * @param parameters generation parameters controlling output behavior
     * @return generated text and the provider's actual request identity (never null)
     * @throws IllegalArgumentException if prompt is null
     */
    InferenceResult complete(String prompt, GenerationParameters parameters);

    /**
     * Generates completions for a batch of prompts.
     *
     * <p>Batch size must not exceed {@link #maxBatchSize()}. Implementations
     * should optimize for throughput.</p>
     *
     * @param prompts    the prompts to complete (must not be null or empty; size &le; maxBatchSize)
     * @param parameters shared generation parameters
     * @return list of inference results, one per input prompt, in order (never null)
     * @throws IllegalArgumentException if prompts is null, empty, or exceeds maxBatchSize
     */
    List<InferenceResult> completeBatch(List<String> prompts, GenerationParameters parameters);

    /**
     * Returns the model name this provider serves.
     *
     * @return the model name (never null or blank)
     */
    String modelName();

    /**
     * Returns the maximum number of prompts per batch request.
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

    /**
     * Exact serving limits for this provider deployment. Implementations must
     * override this unless they override {@link #capabilities()} with limits
     * obtained from a governed model descriptor.
     */
    default AiCapabilityDescriptor.ResourceLimits resourceLimits() {
        throw new IllegalStateException(
                "Inference provider must declare exact resource limits");
    }

    /**
     * Execution capabilities advertised by this provider. Providers with
     * additional limits should override this method; callers reject operations
     * not represented here.
     */
    default Set<AiCapabilityDescriptor> capabilities() {
        Set<Modality> modalities = supportedModalities();
        AiCapabilityDescriptor.RetryPolicy retryPolicy =
                AiCapabilityDescriptor.RetryPolicy.noRetry();
        var text = new AiCapabilityDescriptor(
                "inference:" + modelName(),
                modelVersion(),
                AiOperation.TEXT_GENERATION,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VARCHAR", List.of("VARCHAR"))),
                Set.of(AiExecutionMode.SCALAR,
                        AiExecutionMode.BATCH,
                        AiExecutionMode.SYNC),
                modalities,
                maxBatchSize(),
                resourceLimits(),
                false,
                retryPolicy,
                Set.of("AI_MODEL_USAGE"));
        if (modalities.equals(Set.of(Modality.TEXT))) {
            return Set.of(text);
        }
        var multimodal = new AiCapabilityDescriptor(
                "multimodal-inference:" + modelName(),
                modelVersion(),
                AiOperation.MULTIMODAL_GENERATION,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VARCHAR", List.of("MULTIMODAL_PROMPT"))),
                Set.of(AiExecutionMode.SCALAR,
                        AiExecutionMode.SYNC),
                modalities,
                1,
                resourceLimits(),
                false,
                retryPolicy,
                Set.of("AI_MODEL_USAGE"));
        return Set.of(text, multimodal);
    }

    // ---- Multimodal capability ----

    /**
     * Generates a completion for a multimodal prompt containing mixed content (text + images/files).
     * Providers must override this method when they advertise non-text modalities.
     * The default fails closed so binary content is never silently discarded.
     *
     * @param prompt     the multimodal prompt (must not be null)
     * @param parameters generation parameters
     * @return inference result (never null)
     */
    default InferenceResult completeMultimodal(MultimodalPrompt prompt, GenerationParameters parameters) {
        throw new AiInvocationException(
                AiFailureCategory.CAPABILITY_MISMATCH,
                false,
                "Inference provider '" + modelName() + "' does not support multimodal input");
    }

    /**
     * Returns the set of input modalities supported by this provider.
     * Default returns TEXT only. Override to advertise multimodal capabilities.
     *
     * @return set of supported modalities (never null or empty)
     */
    default Set<Modality> supportedModalities() {
        return Set.of(Modality.TEXT);
    }
}
