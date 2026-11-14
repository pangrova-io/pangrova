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
package org.pangrova.ai.api.capability;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Exact per-request serving limits, not usage statistics or a cumulative token budget.
 * Bytes refer to encoded HTTP entity bodies, including protocol framing and prefixes.
 * Token limits constrain the model request; they do not estimate bytes or admit retries.
 */
@Experimental
public record AiRequestConstraints(
        long maxInputTokens,
        long maxOutputTokens,
        long maxRequestBytes,
        long maxResponseBytes) {

    public AiRequestConstraints {
        if (maxInputTokens <= 0 || maxOutputTokens < 0
                || maxRequestBytes <= 0 || maxResponseBytes <= 0) {
            throw new IllegalArgumentException(
                    "AI request input-token and body-byte limits must be positive; output-token limit may be zero");
        }
    }

    /** Rejects an oversized encoded request before transport admission or send. */
    public void requireRequestBytes(long encodedBytes) {
        requireBytes(encodedBytes, maxRequestBytes, "request");
    }

    /** Rejects an oversized response body before retaining the offending chunk. */
    public void requireResponseBytes(long receivedBytes) {
        requireBytes(receivedBytes, maxResponseBytes, "response");
    }

    private static void requireBytes(long bytes, long limit, String direction) {
        if (bytes < 0) {
            throw new IllegalArgumentException("encoded body bytes must not be negative");
        }
        if (bytes > limit) {
            throw new AiInvocationException(AiFailureCategory.BUDGET_EXCEEDED,
                    false, "AI provider " + direction + " body exceeds its exact per-request byte limit");
        }
    }
}
