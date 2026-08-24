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
 * Token usage statistics returned by an inference provider call.
 *
 * @param inputTokens  number of tokens in the prompt (must be non-negative)
 * @param outputTokens number of tokens in the completion (must be non-negative)
 * @param totalTokens  total tokens consumed (input + output)
 */
@Experimental
public record TokenUsage(long inputTokens, long outputTokens, long totalTokens) {

    public static final TokenUsage ZERO = new TokenUsage(0, 0, 0);

    public TokenUsage {
        if (inputTokens < 0) {
            throw new IllegalArgumentException("inputTokens must be non-negative");
        }
        if (outputTokens < 0) {
            throw new IllegalArgumentException("outputTokens must be non-negative");
        }
        totalTokens = inputTokens + outputTokens;
    }

    public static TokenUsage of(long inputTokens, long outputTokens) {
        return new TokenUsage(inputTokens, outputTokens, inputTokens + outputTokens);
    }
}
