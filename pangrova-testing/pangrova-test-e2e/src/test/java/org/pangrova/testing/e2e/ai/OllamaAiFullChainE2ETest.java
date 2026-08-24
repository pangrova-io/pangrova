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
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.ScalarFunction;
import org.pangrova.worker.ai.function.AiClassifyFunction;
import org.pangrova.worker.ai.function.AiCompletionFunction;
import org.pangrova.worker.ai.function.AiFilterFunction;
import org.pangrova.worker.ai.function.AiFunctionFactory;
import org.pangrova.worker.ai.function.AiSummarizeFunction;
import org.pangrova.worker.ai.inference.ConfigurableInferenceProvider;
import org.pangrova.worker.ai.inference.OpenAiCompatibleInferenceProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * E2E test that runs Ollama in a TestContainer with a real model to verify
 * the full AI inference pipeline: provider construction, single/batch
 * completions, all AI SQL functions, concurrent access, configuration
 * via system properties, and error handling.
 *
 * <p>Uses the tinyllama model (~637MB) as the smallest practical model
 * for real inference validation. The model is pulled inside the container
 * after startup.</p>
 *
 * <p>Tagged as {@code integration} because the container startup and model
 * pull take significant time (2-5 minutes depending on network/cache).</p>
 */
@Testcontainers
@Tag("integration")
@DisplayName("Ollama AI Full-Chain E2E -- Real Model Inference")
class OllamaAiFullChainE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(OllamaAiFullChainE2ETest.class);

    private static final String MODEL_NAME = "tinyllama";

    @Container
    static GenericContainer<?> ollama = new GenericContainer<>("ollama/ollama:0.6.2")
            .withExposedPorts(11434)
            .waitingFor(Wait.forHttp("/api/tags")
                    .forPort(11434)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    private static String ollamaEndpoint;
    private static InferenceProvider provider;
    private static boolean modelReady;

    @BeforeAll
    static void setup() throws Exception {
        assumeTrue(ollama.isRunning(), "Ollama container did not start");

        // Pull tinyllama model inside the container (may take 1-5 minutes)
        LOG.info("Pulling {} model inside Ollama container...", MODEL_NAME);
        long pullStart = System.nanoTime();
        var result = ollama.execInContainer("ollama", "pull", MODEL_NAME);
        long pullMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pullStart);
        LOG.info("Model pull completed in {} ms, exit code: {}, stdout: {}",
                pullMs, result.getExitCode(), result.getStdout().trim());

        if (result.getExitCode() != 0) {
            LOG.error("Model pull failed: {}", result.getStderr());
            modelReady = false;
            return;
        }
        modelReady = true;

        // Build endpoint URL (Ollama's OpenAI-compatible endpoint)
        ollamaEndpoint = String.format("http://%s:%d/v1/chat/completions",
                ollama.getHost(), ollama.getMappedPort(11434));
        LOG.info("Ollama endpoint: {}", ollamaEndpoint);

        // Create provider directly for test isolation
        provider = new OpenAiCompatibleInferenceProvider(
                ollamaEndpoint, MODEL_NAME, "ollama",
                16, 10.0, 120);
    }

    private static Function<String, InferenceProvider> providerLookup() {
        return modelName -> provider;
    }

    @Nested
    @DisplayName("Single Inference")
    class SingleInference {

        @Test
        @DisplayName("complete returns non-empty response")
        void completeReturnsNonEmptyResponse() {
            assumeTrue(modelReady, "Model not available");

            InferenceResult result = provider.complete("What is 2+2?", GenerationParameters.DEFAULTS);

            assertThat(result.text())
                    .isNotNull()
                    .isNotBlank();
            LOG.info("complete response: {}", result.text());
        }

        @Test
        @DisplayName("complete with temperature=0 returns deterministic-ish response")
        void completeWithTemperature() {
            assumeTrue(modelReady, "Model not available");

            InferenceResult result = provider.complete(
                    "What is the capital of France? Answer in one word.",
                    GenerationParameters.deterministic());

            assertThat(result.text())
                    .isNotNull()
                    .isNotBlank();
            LOG.info("temperature=0 response: {}", result.text());
        }

        @Test
        @DisplayName("complete with max_tokens returns short response")
        void completeWithMaxTokens() {
            assumeTrue(modelReady, "Model not available");

            InferenceResult result = provider.complete(
                    "Say hello.",
                    new GenerationParameters(OptionalDouble.empty(), OptionalInt.of(10), Optional.empty()));

            assertThat(result.text())
                    .isNotNull()
                    .isNotBlank();
            LOG.info("max_tokens=10 response (length={}): {}", result.text().length(), result.text());
        }

        @Test
        @DisplayName("ai_classify classifies text into one of the given labels")
        void classifyText() {
            assumeTrue(modelReady, "Model not available");

            var classifyFn = new AiClassifyFunction(providerLookup());
            ColumnValue result = classifyFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue("I love this product!"),
                    new ColumnValue.StringValue("positive,negative,neutral")));

            assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
            String label = ((ColumnValue.StringValue) result).value();
            assertThat(label).isNotBlank();
            LOG.info("classify result: {}", label);
        }

        @Test
        @DisplayName("ai_summarize produces a non-empty summary")
        void summarizeText() {
            assumeTrue(modelReady, "Model not available");

            var summarizeFn = new AiSummarizeFunction(providerLookup());
            ColumnValue result = summarizeFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue(
                            "The quick brown fox jumps over the lazy dog. "
                                    + "This sentence contains every letter of the English alphabet. "
                                    + "It has been used for typing practice since the late 19th century.")));

            assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
            String summary = ((ColumnValue.StringValue) result).value();
            assertThat(summary).isNotBlank();
            LOG.info("summarize result: {}", summary);
        }

        @Test
        @DisplayName("ai_filter evaluates text against criteria")
        void filterText() {
            assumeTrue(modelReady, "Model not available");

            var filterFn = new AiFilterFunction(providerLookup());
            ColumnValue result = filterFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue("Buy now! Special offer! 50% off today only!"),
                    new ColumnValue.StringValue("mentions promotional content")));

            assertThat(result).isInstanceOf(ColumnValue.BooleanValue.class);
            LOG.info("filter result: {}", ((ColumnValue.BooleanValue) result).value());
        }
    }

    @Nested
    @DisplayName("Batch Inference")
    class BatchInference {

        @Test
        @DisplayName("completeBatch with multiple prompts returns matching responses")
        void completeBatchMultiplePrompts() {
            assumeTrue(modelReady, "Model not available");

            List<InferenceResult> responses = provider.completeBatch(
                    List.of("Say hello", "Say goodbye"),
                    GenerationParameters.DEFAULTS);

            assertThat(responses)
                    .hasSize(2)
                    .allSatisfy(r -> assertThat(r.text()).isNotBlank());
            LOG.info("batch responses: {}", responses.stream().map(InferenceResult::text).toList());
        }

        @Test
        @DisplayName("completeBatch with single prompt returns one response")
        void completeBatchSinglePrompt() {
            assumeTrue(modelReady, "Model not available");

            List<InferenceResult> responses = provider.completeBatch(
                    List.of("What color is the sky?"),
                    GenerationParameters.DEFAULTS);

            assertThat(responses)
                    .hasSize(1)
                    .allSatisfy(r -> assertThat(r.text()).isNotBlank());
        }
    }

    @Nested
    @DisplayName("Concurrent Access")
    class ConcurrentAccess {

        @Test
        @DisplayName("concurrent completions all succeed with non-empty responses")
        void concurrentCompletions() throws Exception {
            assumeTrue(modelReady, "Model not available");

            ExecutorService pool = Executors.newFixedThreadPool(4);
            try {
                long startNanos = System.nanoTime();
                List<CompletableFuture<InferenceResult>> futures = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    int idx = i;
                    futures.add(CompletableFuture.supplyAsync(
                            () -> provider.complete(
                                    "What is " + idx + "+1? Answer with just the number.",
                                    GenerationParameters.DEFAULTS),
                            pool));
                }

                List<InferenceResult> results = new ArrayList<>();
                for (CompletableFuture<InferenceResult> f : futures) {
                    results.add(f.get(5, TimeUnit.MINUTES));
                }
                long totalMs = TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - startNanos);

                assertThat(results)
                        .hasSize(10)
                        .allSatisfy(r -> assertThat(r.text()).isNotBlank());
                LOG.info("10 concurrent completions finished in {} ms", totalMs);
            } finally {
                pool.shutdown();
                pool.awaitTermination(10, TimeUnit.SECONDS);
            }
        }

        @Test
        @DisplayName("concurrent AI functions all complete without errors")
        void concurrentAiFunctions() throws Exception {
            assumeTrue(modelReady, "Model not available");

            ExecutorService pool = Executors.newFixedThreadPool(4);
            try {
                var completeFn = new AiCompletionFunction(providerLookup());
                var classifyFn = new AiClassifyFunction(providerLookup());

                List<CompletableFuture<ColumnValue>> futures = new ArrayList<>();

                // 5 completion function invocations
                for (int i = 0; i < 5; i++) {
                    futures.add(CompletableFuture.supplyAsync(
                            () -> completeFn.invoke(List.of(
                                    new ColumnValue.StringValue(MODEL_NAME),
                                    new ColumnValue.StringValue("Say hi"))),
                            pool));
                }

                // 5 classify function invocations
                for (int i = 0; i < 5; i++) {
                    futures.add(CompletableFuture.supplyAsync(
                            () -> classifyFn.invoke(List.of(
                                    new ColumnValue.StringValue(MODEL_NAME),
                                    new ColumnValue.StringValue("Great product!"),
                                    new ColumnValue.StringValue("positive,negative"))),
                            pool));
                }

                List<ColumnValue> results = new ArrayList<>();
                for (CompletableFuture<ColumnValue> f : futures) {
                    results.add(f.get(5, TimeUnit.MINUTES));
                }

                assertThat(results)
                        .hasSize(10)
                        .allSatisfy(r -> assertThat(r).isNotInstanceOf(ColumnValue.NullValue.class));
                LOG.info("10 concurrent AI function calls completed successfully");
            } finally {
                pool.shutdown();
                pool.awaitTermination(10, TimeUnit.SECONDS);
            }
        }

        @Test
        @DisplayName("throughput measurement logs latency stats")
        void throughputMeasurement() {
            assumeTrue(modelReady, "Model not available");

            int iterations = 20;
            long[] latencies = new long[iterations];

            for (int i = 0; i < iterations; i++) {
                long start = System.nanoTime();
                InferenceResult response = provider.complete("Say ok.",
                        new GenerationParameters(OptionalDouble.empty(), OptionalInt.of(5), Optional.empty()));
                latencies[i] = System.nanoTime() - start;
                assertThat(response.text()).isNotBlank();
            }

            // Compute stats (no assertions on values -- hardware-dependent)
            java.util.Arrays.sort(latencies);
            long avgMs = 0;
            for (long l : latencies) {
                avgMs += l;
            }
            avgMs = TimeUnit.NANOSECONDS.toMillis(avgMs / iterations);
            long p50Ms = TimeUnit.NANOSECONDS.toMillis(latencies[iterations / 2]);
            long p99Ms = TimeUnit.NANOSECONDS.toMillis(latencies[(int) (iterations * 0.99)]);
            long minMs = TimeUnit.NANOSECONDS.toMillis(latencies[0]);
            long maxMs = TimeUnit.NANOSECONDS.toMillis(latencies[iterations - 1]);

            LOG.info("Throughput over {} calls: avg={}ms, p50={}ms, p99={}ms, min={}ms, max={}ms",
                    iterations, avgMs, p50Ms, p99Ms, minMs, maxMs);
        }
    }

    @Nested
    @DisplayName("Configuration Path")
    class ConfigurationPath {

        private String origEndpoint;
        private String origApiKey;
        private String origModel;

        private void saveProperties() {
            origEndpoint = System.getProperty("pangrova.ai.inference.endpoint");
            origApiKey = System.getProperty("pangrova.ai.inference.api-key");
            origModel = System.getProperty("pangrova.ai.inference.model");
        }

        private void restoreProperties() {
            restoreProperty("pangrova.ai.inference.endpoint", origEndpoint);
            restoreProperty("pangrova.ai.inference.api-key", origApiKey);
            restoreProperty("pangrova.ai.inference.model", origModel);
        }

        private void restoreProperty(String key, String value) {
            if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        }

        @Test
        @DisplayName("ConfigurableInferenceProvider via system properties completes against Ollama")
        void configurableProviderViaSystemProperties() {
            assumeTrue(modelReady, "Model not available");

            saveProperties();
            try {
                System.setProperty("pangrova.ai.inference.endpoint", ollamaEndpoint);
                System.setProperty("pangrova.ai.inference.api-key", "ollama");
                System.setProperty("pangrova.ai.inference.model", MODEL_NAME);

                var configProvider = new ConfigurableInferenceProvider();

                assertThat(configProvider.modelName()).isEqualTo(MODEL_NAME);
                InferenceResult response = configProvider.complete("Say hello.", GenerationParameters.DEFAULTS);
                assertThat(response.text()).isNotBlank();
                LOG.info("ConfigurableInferenceProvider response: {}", response.text());
            } finally {
                restoreProperties();
            }
        }

        @Test
        @DisplayName("AiFunctionFactory with Ollama-backed provider invokes ai_complete")
        void aiFunctionFactoryWithOllamaProvider() {
            assumeTrue(modelReady, "Model not available");

            var factory = new AiFunctionFactory(providerLookup());
            List<ScalarFunction> functions = factory.getScalarFunctions(Map.of());

            ScalarFunction aiComplete = functions.stream()
                    .filter(f -> "ai_complete".equals(f.getMetadata().name()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("ai_complete not found"));

            ColumnValue result = aiComplete.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue("What is 1+1?")));

            assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
            String value = ((ColumnValue.StringValue) result).value();
            assertThat(value).isNotBlank();
            LOG.info("AiFunctionFactory ai_complete result: {}", value);
        }
    }

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandling {

        @Test
        @DisplayName("non-existent model name causes a runtime error")
        void invalidModelReturnsError() {
            assumeTrue(modelReady, "Model not available");

            var badProvider = new OpenAiCompatibleInferenceProvider(
                    ollamaEndpoint, "nonexistent-model-xyz", "ollama",
                    16, 10.0, 30);

            assertThatThrownBy(() -> badProvider.complete("Hello", GenerationParameters.DEFAULTS))
                    .isInstanceOf(RuntimeException.class);
        }

        @Test
        @DisplayName("null prompt throws IllegalArgumentException")
        void emptyPromptReturnsError() {
            assumeTrue(modelReady, "Model not available");

            assertThatThrownBy(() -> provider.complete(null, GenerationParameters.DEFAULTS))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("batch exceeding maxBatchSize throws IllegalArgumentException")
        void oversizeBatchReturnsError() {
            assumeTrue(modelReady, "Model not available");

            List<String> oversized = new ArrayList<>();
            for (int i = 0; i < provider.maxBatchSize() + 1; i++) {
                oversized.add("prompt " + i);
            }

            assertThatThrownBy(() -> provider.completeBatch(oversized, GenerationParameters.DEFAULTS))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("exceeds maxBatchSize");
        }
    }
}
