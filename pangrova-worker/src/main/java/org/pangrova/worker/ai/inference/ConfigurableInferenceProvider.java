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
package org.pangrova.worker.ai.inference;

import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.ai.inference.InferenceResult;

import java.util.List;

/**
 * ServiceLoader-discoverable inference provider that reads configuration from
 * system properties.
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code pangrova.ai.inference.endpoint} — the OpenAI-compatible endpoint
 *       (default: {@code http://localhost:11434/v1})</li>
 *   <li>{@code pangrova.ai.inference.api-key} — the API key
 *       (default: empty string, which sends an empty Bearer token)</li>
 *   <li>{@code pangrova.ai.inference.model} — the model name
 *       (default: {@code llama3})</li>
 * </ul>
 *
 * <p>Delegates all operations to an {@link OpenAiCompatibleInferenceProvider}
 * constructed from these properties.</p>
 *
 * <p>Thread-safe: the delegate is thread-safe and constructed once at init.</p>
 */
public final class ConfigurableInferenceProvider implements InferenceProvider {

    private static final String PROP_ENDPOINT = "pangrova.ai.inference.endpoint";
    private static final String PROP_API_KEY = "pangrova.ai.inference.api-key";
    private static final String PROP_MODEL = "pangrova.ai.inference.model";

    private static final String DEFAULT_ENDPOINT = "http://localhost:11434/v1";
    private static final String DEFAULT_API_KEY = "";
    private static final String DEFAULT_MODEL = "llama3";

    private static final int MAX_BATCH_SIZE = 16;
    private static final double MAX_REQUESTS_PER_SECOND = 10;
    private static final int TIMEOUT_SECONDS = 30;

    private final OpenAiCompatibleInferenceProvider delegate;

    /**
     * No-arg constructor for {@link java.util.ServiceLoader} discovery.
     */
    public ConfigurableInferenceProvider() {
        String endpoint = System.getProperty(PROP_ENDPOINT, DEFAULT_ENDPOINT);
        String apiKey = System.getProperty(PROP_API_KEY, DEFAULT_API_KEY);
        String model = System.getProperty(PROP_MODEL, DEFAULT_MODEL);

        if (apiKey.isEmpty()) {
            apiKey = "no-key";
        }

        this.delegate = new OpenAiCompatibleInferenceProvider(
                endpoint, model, apiKey,
                MAX_BATCH_SIZE, MAX_REQUESTS_PER_SECOND, TIMEOUT_SECONDS);
    }

    @Override
    public InferenceResult complete(String prompt, GenerationParameters parameters) {
        return delegate.complete(prompt, parameters);
    }

    @Override
    public List<InferenceResult> completeBatch(List<String> prompts, GenerationParameters parameters) {
        return delegate.completeBatch(prompts, parameters);
    }

    @Override
    public String modelName() {
        return delegate.modelName();
    }

    @Override
    public int maxBatchSize() {
        return delegate.maxBatchSize();
    }
}
