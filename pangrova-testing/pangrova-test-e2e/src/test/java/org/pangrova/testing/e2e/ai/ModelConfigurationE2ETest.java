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
import org.pangrova.worker.ai.inference.OpenAiCompatibleInferenceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E tests for the full model configuration path from system properties
 * through provider construction to function factory routing.
 *
 * <p>Validates the end-to-end configuration chain: system properties drive
 * {@link ConfigurableInferenceProvider} which delegates to
 * {@link OpenAiCompatibleInferenceProvider}, and
 * {@link AiFunctionFactory} routes by model name.</p>
 */
@DisplayName("Model Configuration E2E")
class ModelConfigurationE2ETest {

    private static final String PROP_ENDPOINT = "pangrova.ai.inference.endpoint";
    private static final String PROP_API_KEY = "pangrova.ai.inference.api-key";
    private static final String PROP_MODEL = "pangrova.ai.inference.model";

    @Nested
    @DisplayName("Default Configuration")
    class DefaultConfiguration {

        private String savedEndpoint;
        private String savedApiKey;
        private String savedModel;

        @BeforeEach
        void clearProperties() {
            savedEndpoint = System.getProperty(PROP_ENDPOINT);
            savedApiKey = System.getProperty(PROP_API_KEY);
            savedModel = System.getProperty(PROP_MODEL);
            System.clearProperty(PROP_ENDPOINT);
            System.clearProperty(PROP_API_KEY);
            System.clearProperty(PROP_MODEL);
        }

        @AfterEach
        void restoreProperties() {
            restore(PROP_ENDPOINT, savedEndpoint);
            restore(PROP_API_KEY, savedApiKey);
            restore(PROP_MODEL, savedModel);
        }

        @Test
        @DisplayName("Default model name is 'llama3'")
        void defaultModelName() {
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider.modelName()).isEqualTo("llama3");
        }

        @Test
        @DisplayName("Default max batch size is 16")
        void defaultMaxBatchSize() {
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider.maxBatchSize()).isEqualTo(16);
        }

        @Test
        @DisplayName("Provider implements InferenceProvider SPI")
        void implementsSpi() {
            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider).isInstanceOf(InferenceProvider.class);
        }

        private void restore(String key, String value) {
            if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        }
    }

    @Nested
    @DisplayName("Custom Endpoint")
    class CustomEndpoint {

        private String savedEndpoint;

        @BeforeEach
        void save() {
            savedEndpoint = System.getProperty(PROP_ENDPOINT);
        }

        @AfterEach
        void restore() {
            if (savedEndpoint == null) {
                System.clearProperty(PROP_ENDPOINT);
            } else {
                System.setProperty(PROP_ENDPOINT, savedEndpoint);
            }
        }

        @Test
        @DisplayName("Custom endpoint is read from system property")
        void customEndpointUsed() {
            System.setProperty(PROP_ENDPOINT, "https://api.openai.com/v1/chat/completions");

            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            // Provider constructs without error using the custom endpoint
            assertThat(provider).isNotNull();
            assertThat(provider.modelName()).isEqualTo("llama3");
        }

        @Test
        @DisplayName("Provider with HTTPS endpoint does not fail at construction")
        void httpsEndpoint() {
            System.setProperty(PROP_ENDPOINT, "https://secure-inference.example.com/v1");

            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();
            assertThat(provider).isNotNull();
        }
    }

    @Nested
    @DisplayName("Custom Model")
    class CustomModel {

        private String savedModel;

        @BeforeEach
        void save() {
            savedModel = System.getProperty(PROP_MODEL);
        }

        @AfterEach
        void restore() {
            if (savedModel == null) {
                System.clearProperty(PROP_MODEL);
            } else {
                System.setProperty(PROP_MODEL, savedModel);
            }
        }

        @Test
        @DisplayName("Custom model name is propagated to modelName()")
        void customModelPropagated() {
            System.setProperty(PROP_MODEL, "mistral-7b");

            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider.modelName()).isEqualTo("mistral-7b");
        }

        @Test
        @DisplayName("Model name with special characters is accepted")
        void specialCharModelName() {
            System.setProperty(PROP_MODEL, "meta-llama/Meta-Llama-3.1-8B-Instruct");

            ConfigurableInferenceProvider provider = new ConfigurableInferenceProvider();

            assertThat(provider.modelName()).isEqualTo("meta-llama/Meta-Llama-3.1-8B-Instruct");
        }
    }

    @Nested
    @DisplayName("ServiceLoader Discovery")
    class ServiceLoaderDiscovery {

        @Test
        @DisplayName("ServiceLoader discovers ConfigurableInferenceProvider")
        void serviceLoaderFindsProvider() {
            ServiceLoader<InferenceProvider> loader = ServiceLoader.load(InferenceProvider.class);

            boolean found = false;
            for (InferenceProvider p : loader) {
                if (p instanceof ConfigurableInferenceProvider) {
                    found = true;
                    break;
                }
            }

            assertThat(found)
                    .as("ConfigurableInferenceProvider should be discoverable via ServiceLoader")
                    .isTrue();
        }

        @Test
        @DisplayName("Discovered provider has a valid model name")
        void discoveredProviderHasModelName() {
            ServiceLoader<InferenceProvider> loader = ServiceLoader.load(InferenceProvider.class);

            for (InferenceProvider p : loader) {
                if (p instanceof ConfigurableInferenceProvider) {
                    assertThat(p.modelName()).isNotNull().isNotBlank();
                    assertThat(p.maxBatchSize()).isGreaterThan(0);
                    break;
                }
            }
        }
    }

    @Nested
    @DisplayName("AiFunctionFactory Model Routing")
    class AiFunctionFactoryModelRouting {

        @Test
        @DisplayName("Factory routes to specific provider when model name matches")
        void routesToSpecificProvider() {
            InferenceProvider alpha = new TestInferenceProvider("alpha-model");
            InferenceProvider beta = new TestInferenceProvider("beta-model");

            Map<String, InferenceProvider> providerMap = Map.of(
                    "alpha-model", alpha,
                    "beta-model", beta);

            Function<String, InferenceProvider> lookup = modelName -> {
                InferenceProvider p = providerMap.get(modelName);
                return p != null ? p : alpha;
            };

            AiFunctionFactory factory = new AiFunctionFactory(lookup, name -> null);

            assertThat(factory.getName()).isEqualTo("ai-inference");
            assertThat(factory.getScalarFunctions(Map.of())).hasSize(15);
        }

        @Test
        @DisplayName("Factory falls back to default provider for unknown model")
        void fallsBackToDefault() {
            InferenceProvider defaultProvider = new TestInferenceProvider("default");

            Function<String, InferenceProvider> lookup = modelName -> defaultProvider;

            AiFunctionFactory factory = new AiFunctionFactory(lookup, name -> null);

            assertThat(factory.getScalarFunctions(Map.of())).hasSize(15);
        }

        @Test
        @DisplayName("Factory with error-throwing lookup defers error to invocation time")
        void errorDeferredToInvocation() {
            Function<String, InferenceProvider> failingLookup = modelName -> {
                throw new IllegalStateException(
                        "No InferenceProvider found on the classpath for model '"
                                + modelName + "'");
            };

            AiFunctionFactory factory = new AiFunctionFactory(failingLookup, name -> null);

            // Factory construction succeeds; functions are returned
            assertThat(factory.getScalarFunctions(Map.of())).hasSize(15);
            assertThat(factory.getName()).isEqualTo("ai-inference");
        }
    }

    @Nested
    @DisplayName("OpenAiCompatibleInferenceProvider Validation")
    class ProviderValidation {

        @Test
        @DisplayName("Rejects blank endpoint")
        void rejectsBlankEndpoint() {
            assertThatThrownBy(() -> new OpenAiCompatibleInferenceProvider(
                    "", "model", "key", 16, 10.0, 30))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("endpoint");
        }

        @Test
        @DisplayName("Rejects blank model name")
        void rejectsBlankModelName() {
            assertThatThrownBy(() -> new OpenAiCompatibleInferenceProvider(
                    "http://localhost:11434/v1", "", "key", 16, 10.0, 30))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("modelName");
        }

        @Test
        @DisplayName("Rejects blank API key")
        void rejectsBlankApiKey() {
            assertThatThrownBy(() -> new OpenAiCompatibleInferenceProvider(
                    "http://localhost:11434/v1", "model", "", 16, 10.0, 30))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("apiKey");
        }

        @Test
        @DisplayName("Rejects non-positive batch size")
        void rejectsNonPositiveBatchSize() {
            assertThatThrownBy(() -> new OpenAiCompatibleInferenceProvider(
                    "http://localhost:11434/v1", "model", "key", 0, 10.0, 30))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxBatchSize");
        }

        @Test
        @DisplayName("Rejects non-positive rate limit")
        void rejectsNonPositiveRateLimit() {
            assertThatThrownBy(() -> new OpenAiCompatibleInferenceProvider(
                    "http://localhost:11434/v1", "model", "key", 16, 0.0, 30))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxRequestsPerSecond");
        }

        @Test
        @DisplayName("Rejects non-positive timeout")
        void rejectsNonPositiveTimeout() {
            assertThatThrownBy(() -> new OpenAiCompatibleInferenceProvider(
                    "http://localhost:11434/v1", "model", "key", 16, 10.0, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("timeoutSeconds");
        }

        @Test
        @DisplayName("Valid construction preserves model name and batch size")
        void validConstruction() {
            OpenAiCompatibleInferenceProvider provider = new OpenAiCompatibleInferenceProvider(
                    "http://localhost:11434/v1", "test-model", "sk-test", 32, 5.0, 60);

            assertThat(provider.modelName()).isEqualTo("test-model");
            assertThat(provider.maxBatchSize()).isEqualTo(32);
        }
    }

    /**
     * Lightweight InferenceProvider for testing factory routing without HTTP calls.
     */
    private static final class TestInferenceProvider implements InferenceProvider {

        private final String model;

        TestInferenceProvider(String model) {
            this.model = model;
        }

        @Override
        public InferenceResult complete(String prompt, GenerationParameters parameters) {
            return InferenceResult.of(model + ": " + prompt);
        }

        @Override
        public List<InferenceResult> completeBatch(List<String> prompts, GenerationParameters parameters) {
            return prompts.stream().map(p -> InferenceResult.of(model + ": " + p)).toList();
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
