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
package org.pangrova.spi.ai.inference;

import org.pangrova.spi.annotation.Experimental;

/**
 * Result of an inference (LLM completion) call, bundling the generated text
 * with token usage statistics.
 *
 * <p>This enables downstream systems (token budget enforcement, metrics,
 * cost attribution) to track actual token consumption per call without
 * requiring a separate statistics API.</p>
 *
 * @param text       the generated text response (never null)
 * @param tokenUsage token usage statistics (never null; use {@link TokenUsage#ZERO} if unknown)
 */
@Experimental
public record InferenceResult(String text, TokenUsage tokenUsage) {

    public InferenceResult {
        if (text == null) {
            throw new IllegalArgumentException("text must not be null");
        }
        if (tokenUsage == null) {
            throw new IllegalArgumentException("tokenUsage must not be null");
        }
    }

    /**
     * Creates a result with zero/unknown token usage.
     *
     * @param text the generated text
     * @return inference result with {@link TokenUsage#ZERO}
     */
    public static InferenceResult of(String text) {
        return new InferenceResult(text, TokenUsage.ZERO);
    }

    /**
     * Creates a result with known token usage.
     *
     * @param text         the generated text
     * @param inputTokens  number of input tokens consumed
     * @param outputTokens number of output tokens produced
     * @return inference result with token usage
     */
    public static InferenceResult of(String text, long inputTokens, long outputTokens) {
        return new InferenceResult(text, TokenUsage.of(inputTokens, outputTokens));
    }
}
