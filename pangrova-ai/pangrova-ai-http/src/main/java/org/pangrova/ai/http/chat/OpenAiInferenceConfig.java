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
package org.pangrova.ai.http.chat;

import org.pangrova.ai.http.AiModelProperties;
import org.pangrova.ai.api.model.AiModelDescriptor;

/** Immutable configuration for an OpenAI-compatible inference deployment. */
public record OpenAiInferenceConfig(
        String endpoint,
        String modelName,
        String apiKey,
        int maxBatchSize,
        double maxRequestsPerSecond,
        int maxConcurrency,
        int timeoutSeconds) {
    /** Resolves transport limits from a canonical deployment descriptor. */
    public static OpenAiInferenceConfig fromModel(AiModelDescriptor model, String apiKey) {
        int maxBatchSize = AiModelProperties.integer(model, "max-batch-size", 16);
        double maxRequestsPerSecond = AiModelProperties.positiveDouble(
                model, "max-requests-per-second", 10.0);
        int timeoutSeconds = AiModelProperties.integer(
                model, "timeout-seconds", 30);
        return new OpenAiInferenceConfig(
                model.endpoint().toString(), model.properties().get("provider-model"), apiKey,
                maxBatchSize, maxRequestsPerSecond,
                AiModelProperties.integer(model, "max-concurrency", -1),
                timeoutSeconds);
    }

    @Override
    public String toString() {
        return "OpenAiInferenceConfig[modelName=" + modelName + ", maxBatchSize=" + maxBatchSize
                + ", maxConcurrency=" + maxConcurrency + ", timeoutSeconds=" + timeoutSeconds + "]";
    }
}
