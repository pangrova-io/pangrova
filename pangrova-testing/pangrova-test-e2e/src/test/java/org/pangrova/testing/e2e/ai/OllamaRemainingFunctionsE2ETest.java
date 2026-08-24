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

import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.worker.ai.function.AiExtractFunction;
import org.pangrova.worker.ai.function.AiGuardFunction;
import org.pangrova.worker.ai.function.AiPredictFunction;
import org.pangrova.worker.ai.function.AiTranslateFunction;
import org.pangrova.worker.ai.inference.OpenAiCompatibleInferenceProvider;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * E2E tests for the remaining AI SQL functions (ai_extract, ai_guard, ai_predict,
 * ai_translate) using a real Ollama container with the tinyllama model.
 *
 * <p>Complements {@link OllamaAiFullChainE2ETest} which covers ai_complete,
 * ai_classify, ai_summarize, and ai_filter.</p>
 *
 * <p>Tagged as {@code integration} because the container startup and model
 * pull take significant time (2-5 minutes).</p>
 */
@Testcontainers
@Tag("integration")
@DisplayName("Ollama Remaining AI Functions E2E")
class OllamaRemainingFunctionsE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(OllamaRemainingFunctionsE2ETest.class);
    private static final String MODEL_NAME = "tinyllama";

    @Container
    static GenericContainer<?> ollama = new GenericContainer<>("ollama/ollama:0.6.2")
            .withExposedPorts(11434)
            .waitingFor(Wait.forHttp("/api/tags")
                    .forPort(11434)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    private static InferenceProvider provider;
    private static boolean modelReady;

    @BeforeAll
    static void setup() throws Exception {
        assumeTrue(ollama.isRunning(), "Ollama container did not start");

        LOG.info("Pulling {} model inside Ollama container...", MODEL_NAME);
        long pullStart = System.nanoTime();
        var result = ollama.execInContainer("ollama", "pull", MODEL_NAME);
        long pullMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pullStart);
        LOG.info("Model pull completed in {} ms, exit code: {}", pullMs, result.getExitCode());

        if (result.getExitCode() != 0) {
            LOG.error("Model pull failed: {}", result.getStderr());
            modelReady = false;
            return;
        }
        modelReady = true;

        String endpoint = String.format("http://%s:%d/v1/chat/completions",
                ollama.getHost(), ollama.getMappedPort(11434));
        provider = new OpenAiCompatibleInferenceProvider(
                endpoint, MODEL_NAME, "ollama", 16, 10.0, 120);
    }

    private static Function<String, InferenceProvider> providerLookup() {
        return modelName -> provider;
    }

    // -----------------------------------------------------------------------
    // ai_extract
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("ai_extract - Structured Information Extraction")
    class AiExtractTests {

        @Test
        @DisplayName("Extracts structured data from text")
        void extractStructuredData() {
            assumeTrue(modelReady, "Model not available");

            var extractFn = new AiExtractFunction(providerLookup());
            ColumnValue result = extractFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue("John Smith is 30 years old and lives in New York."),
                    new ColumnValue.StringValue("{\"name\": \"string\", \"age\": \"int\", \"city\": \"string\"}")));

            assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
            String json = ((ColumnValue.StringValue) result).value();
            assertThat(json).isNotBlank();
            LOG.info("ai_extract result: {}", json);
        }

        @Test
        @DisplayName("Returns NullValue for null input text")
        void nullInputReturnsNull() {
            assumeTrue(modelReady, "Model not available");

            var extractFn = new AiExtractFunction(providerLookup());
            ColumnValue result = extractFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.NullValue(),
                    new ColumnValue.StringValue("{\"name\": \"string\"}")));

            assertThat(result).isInstanceOf(ColumnValue.NullValue.class);
        }
    }

    // -----------------------------------------------------------------------
    // ai_guard
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("ai_guard - Content Safety Evaluation")
    class AiGuardTests {

        @Test
        @DisplayName("Evaluates safe content against policy")
        void evaluatesSafeContent() {
            assumeTrue(modelReady, "Model not available");

            var guardFn = new AiGuardFunction(providerLookup());
            ColumnValue result = guardFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue("The weather is beautiful today."),
                    new ColumnValue.StringValue("no hate speech or offensive content")));

            assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
            String assessment = ((ColumnValue.StringValue) result).value();
            assertThat(assessment).isNotBlank();
            LOG.info("ai_guard safe result: {}", assessment);
        }

        @Test
        @DisplayName("Evaluates content with specific policy criteria")
        void evaluatesWithSpecificPolicy() {
            assumeTrue(modelReady, "Model not available");

            var guardFn = new AiGuardFunction(providerLookup());
            ColumnValue result = guardFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue("Buy now! Limited time offer! Click here for free prizes!"),
                    new ColumnValue.StringValue("no spam or promotional content")));

            assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
            String assessment = ((ColumnValue.StringValue) result).value();
            assertThat(assessment).isNotBlank();
            LOG.info("ai_guard spam result: {}", assessment);
        }
    }

    // -----------------------------------------------------------------------
    // ai_predict
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("ai_predict - Prediction Based on Features")
    class AiPredictTests {

        @Test
        @DisplayName("Generates prediction from feature text")
        void predictFromFeatures() {
            assumeTrue(modelReady, "Model not available");

            var predictFn = new AiPredictFunction(providerLookup());
            ColumnValue result = predictFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue(
                            "temperature=35C, humidity=90%, wind_speed=5km/h, cloud_cover=80%")));

            assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
            String prediction = ((ColumnValue.StringValue) result).value();
            assertThat(prediction).isNotBlank();
            LOG.info("ai_predict result: {}", prediction);
        }

        @Test
        @DisplayName("Returns NullValue for null features")
        void nullFeaturesReturnsNull() {
            assumeTrue(modelReady, "Model not available");

            var predictFn = new AiPredictFunction(providerLookup());
            ColumnValue result = predictFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.NullValue()));

            assertThat(result).isInstanceOf(ColumnValue.NullValue.class);
        }
    }

    // -----------------------------------------------------------------------
    // ai_translate
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("ai_translate - Text Translation")
    class AiTranslateTests {

        @Test
        @DisplayName("Translates English text to Spanish")
        void translateToSpanish() {
            assumeTrue(modelReady, "Model not available");

            var translateFn = new AiTranslateFunction(providerLookup());
            ColumnValue result = translateFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue("Hello, how are you?"),
                    new ColumnValue.StringValue("Spanish")));

            assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
            String translation = ((ColumnValue.StringValue) result).value();
            assertThat(translation).isNotBlank();
            LOG.info("ai_translate to Spanish: {}", translation);
        }

        @Test
        @DisplayName("Translates English text to French")
        void translateToFrench() {
            assumeTrue(modelReady, "Model not available");

            var translateFn = new AiTranslateFunction(providerLookup());
            ColumnValue result = translateFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue("Good morning"),
                    new ColumnValue.StringValue("French")));

            assertThat(result).isInstanceOf(ColumnValue.StringValue.class);
            String translation = ((ColumnValue.StringValue) result).value();
            assertThat(translation).isNotBlank();
            LOG.info("ai_translate to French: {}", translation);
        }

        @Test
        @DisplayName("Returns NullValue for null target language")
        void nullLanguageReturnsNull() {
            assumeTrue(modelReady, "Model not available");

            var translateFn = new AiTranslateFunction(providerLookup());
            ColumnValue result = translateFn.invoke(List.of(
                    new ColumnValue.StringValue(MODEL_NAME),
                    new ColumnValue.StringValue("Hello"),
                    new ColumnValue.NullValue()));

            assertThat(result).isInstanceOf(ColumnValue.NullValue.class);
        }
    }
}
