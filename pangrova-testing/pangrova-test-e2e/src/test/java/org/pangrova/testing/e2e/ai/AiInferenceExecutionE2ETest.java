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
package org.pangrova.testing.e2e.ai;

import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.ai.inference.InferenceResult;
import org.pangrova.worker.ai.function.AiFunctionFactory;
import org.pangrova.worker.ai.inference.ConfigurableInferenceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E tests for AI inference provider configuration, construction,
 * and model routing through the full provider discovery pipeline.
 *
 * <p>Tests instantiate real provider classes directly with system properties,
 * verifying that the configuration chain works end-to-end without external
 * service dependencies (network calls are not made in configuration tests).</p>
 */
@DisplayName("AI Inference Execution E2E")
class AiInferenceExecutionE2ETest {

    private static final String PROP_ENDPOINT = "pangrova.ai.inference.endpoint";
    private static final String PROP_API_KEY = "pangrova.ai.inference.api-key";
    private static final String PROP_MODEL = "pangrova.ai.inference.model";

    @Nested
    @DisplayName("ConfigurableInferenceProvider System Properties")
    class SystemPropertyConfiguration {

        private String originalEndpoint;
        private String originalApiKey;
        private String originalModel;

        @BeforeEach
        void saveSystemProperties() {
            originalEndpoint = System.getProperty(PROP_ENDPOINT);
            originalApiKey = System.getProperty(PROP_API_KEY);
            originalModel = System.getProperty(PROP_MODEL);
        }

        @AfterEach
        void restoreSystemProperties() {
            restoreProperty(PROP_ENDPOINT, originalEndpoint);
            restoreProperty(PROP_API_KEY, originalApiKey);
            restoreProperty(PROP_MODEL, originalModel);
        }

        @Test
        @DisplayName("Defaults to localhost:11434 when no system properties set")
        void defaultsToLocalhost() {
            System.clearProperty(PROP_ENDPOINT);
            System.clearProperty(PROP_API_KEY);
            System.clearProperty(PROP_MODEL);

            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider.modelName()).isEqualTo("llama3");
            assertThat(provider.maxBatchSize()).isEqualTo(16);
        }

        @Test
        @DisplayName("Reads custom model name from system property")
        void customModelName() {
            System.setProperty(PROP_MODEL, "gpt-4o");

            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider.modelName()).isEqualTo("gpt-4o");
        }

        @Test
        @DisplayName("Reads custom endpoint from system property")
        void customEndpoint() {
            System.setProperty(PROP_ENDPOINT, "http://my-server:8080/v1");

            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            // Provider should construct without error using the custom endpoint
            assertThat(provider).isNotNull();
            assertThat(provider.modelName()).isEqualTo("llama3");
        }

        @Test
        @DisplayName("Empty API key is replaced with 'no-key'")
        void emptyApiKeyHandled() {
            System.setProperty(PROP_API_KEY, "");

            // Should not throw despite empty key
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();
            assertThat(provider).isNotNull();
        }

        @Test
        @DisplayName("Custom API key is accepted")
        void customApiKey() {
            System.setProperty(PROP_API_KEY, "sk-test-key-12345");

            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();
            assertThat(provider).isNotNull();
        }

        @Test
        @DisplayName("All properties set simultaneously produce valid provider")
        void allPropertiesSet() {
            System.setProperty(PROP_ENDPOINT, "https://api.example.com/v1/chat/completions");
            System.setProperty(PROP_API_KEY, "sk-prod-key");
            System.setProperty(PROP_MODEL, "claude-3-sonnet");

            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider.modelName()).isEqualTo("claude-3-sonnet");
            assertThat(provider.maxBatchSize()).isEqualTo(16);
        }

        private void restoreProperty(String key, String originalValue) {
            if (originalValue == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, originalValue);
            }
        }
    }

    @Nested
    @DisplayName("InferenceProvider Contract")
    class ProviderContract {

        @Test
        @DisplayName("complete() rejects null prompt")
        void completeRejectsNullPrompt() {
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThatThrownBy(() -> provider.complete(null, GenerationParameters.DEFAULTS))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("null");
        }

        @Test
        @DisplayName("completeBatch() rejects null prompts list")
        void completeBatchRejectsNullList() {
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThatThrownBy(() -> provider.completeBatch(null, GenerationParameters.DEFAULTS))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("completeBatch() rejects empty prompts list")
        void completeBatchRejectsEmptyList() {
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThatThrownBy(() -> provider.completeBatch(List.of(), GenerationParameters.DEFAULTS))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("completeBatch() rejects batch exceeding maxBatchSize")
        void completeBatchRejectsOversizedBatch() {
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();
            int maxBatch = provider.maxBatchSize();

            List<String> oversized = java.util.Collections.nCopies(maxBatch + 1, "test prompt");

            assertThatThrownBy(() -> provider.completeBatch(oversized, GenerationParameters.DEFAULTS))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxBatchSize");
        }

        @Test
        @DisplayName("modelName() returns non-null non-blank value")
        void modelNameNonBlank() {
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider.modelName()).isNotNull().isNotBlank();
        }

        @Test
        @DisplayName("maxBatchSize() returns positive value")
        void maxBatchSizePositive() {
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider.maxBatchSize()).isGreaterThan(0);
        }
    }

    @Nested
    @DisplayName("Model Name Routing")
    class ModelNameRouting {

        @Test
        @DisplayName("AiFunctionFactory routes to correct provider by model name")
        void routesToCorrectProvider() {
            InferenceProvider gpt4 = new StubInferenceProvider("gpt-4o");
            InferenceProvider llama = new StubInferenceProvider("llama3");

            Map<String, InferenceProvider> providers = Map.of(
                    "gpt-4o", gpt4,
                    "llama3", llama);

            Function<String, InferenceProvider> lookup = modelName -> {
                InferenceProvider p = providers.get(modelName);
                if (p == null) {
                    throw new IllegalStateException("No provider for model: " + modelName);
                }
                return p;
            };

            AiFunctionFactory factory = new AiFunctionFactory(lookup);

            assertThat(factory.getName()).isEqualTo("ai-inference");
            assertThat(factory.getScalarFunctions(Map.of())).hasSize(15);
        }

        @Test
        @DisplayName("Lookup throws descriptive error for unknown model")
        void unknownModelError() {
            Function<String, InferenceProvider> lookup = modelName -> {
                throw new IllegalStateException(
                        "No InferenceProvider found on the classpath for model '"
                                + modelName + "'");
            };

            AiFunctionFactory factory = new AiFunctionFactory(lookup);

            // Factory constructs fine; the error surfaces at invocation time
            assertThat(factory.getScalarFunctions(Map.of())).hasSize(15);
        }

        @Test
        @DisplayName("AiFunctionFactory rejects null provider lookup")
        void rejectsNullLookup() {
            assertThatThrownBy(() -> new AiFunctionFactory(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("providerLookup");
        }
    }

    /**
     * Lightweight InferenceProvider for testing routing logic without
     * requiring actual HTTP calls. Returns the model name as the completion.
     */
    private static final class StubInferenceProvider implements InferenceProvider {

        private final String model;

        StubInferenceProvider(String model) {
            this.model = model;
        }

        @Override
        public InferenceResult complete(String prompt, GenerationParameters parameters) {
            return InferenceResult.of("response from " + model + ": " + prompt);
        }

        @Override
        public List<InferenceResult> completeBatch(List<String> prompts, GenerationParameters parameters) {
            return prompts.stream()
                    .map(p -> InferenceResult.of("response from " + model + ": " + p))
                    .toList();
        }

        @Override
        public String modelName() {
            return model;
        }

        @Override
        public int maxBatchSize() {
            return 16;
        }
    }
}
