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
package org.pangrova.worker.ai.function;

import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.ai.inference.InferenceResult;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.ScalarFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.worker.ai.blob.GovernedBlobStore;
import org.pangrova.worker.ai.blob.HttpsBlobImporter;
import org.pangrova.worker.ai.blob.BlobTestStores;
import org.pangrova.worker.ai.invocation.AiInvocationObserver;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiFunctionFactoryTest {

    private StubInferenceProvider stubProvider;
    private AiFunctionFactory factory;

    @TempDir
    Path temporaryDirectory;

    @BeforeEach
    void setUp() {
        stubProvider = new StubInferenceProvider();
        GovernedBlobStore store = BlobTestStores.create(
                temporaryDirectory, 1024, Set.of("image/png", "text/plain"));
        factory = new AiFunctionFactory(
                name -> "test-model".equals(name) ? stubProvider : null,
                name -> null,
                name -> null,
                store,
                new HttpsBlobImporter(
                        store, new HttpsBlobImporter.Settings(
                                Set.of(), Set.of("image/png", "text/plain"),
                                Duration.ofSeconds(1), Duration.ofSeconds(1), 1024, 1024,
                                new HttpsBlobImporter.PoolSettings(
                                        8, 4, Duration.ofMinutes(1),
                                        Duration.ofSeconds(10)))));
    }

    @Test
    void observerFanoutContinuesAfterOneObserverFails() {
        AtomicInteger delivered = new AtomicInteger();
        AiInvocationObserver failing = new AiInvocationObserver() {
            @Override
            public void onSuccess(
                    org.pangrova.spi.ai.AiInvocationRequest request,
                    org.pangrova.spi.ai.AiInvocationResult result) {
                throw new IllegalStateException("first failure");
            }

            @Override
            public void onFailure(
                    org.pangrova.spi.ai.AiInvocationRequest request,
                    org.pangrova.spi.ai.AiInvocationException failure) {
                throw new IllegalStateException("first failure");
            }
        };
        AiInvocationObserver succeeding = new AiInvocationObserver() {
            @Override
            public void onSuccess(
                    org.pangrova.spi.ai.AiInvocationRequest request,
                    org.pangrova.spi.ai.AiInvocationResult result) {
                delivered.incrementAndGet();
            }

            @Override
            public void onFailure(
                    org.pangrova.spi.ai.AiInvocationRequest request,
                    org.pangrova.spi.ai.AiInvocationException failure) {
                delivered.incrementAndGet();
            }
        };

        assertThatThrownBy(() -> CompositeAiInvocationObserver.notifyObservers(
                List.of(failing, succeeding),
                observer -> observer.onSuccess(null, null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("first failure");
        assertThat(delivered).hasValue(1);
    }

    @Test
    void factoryPublishesOnlyFunctionsWithExecutableWorkerBindings() {
        List<ScalarFunction> functions = factory.getScalarFunctions(Map.of());
        assertEquals(16, functions.size());
        List<String> names = functions.stream()
                .map(f -> f.getMetadata().name())
                .toList();
        assertTrue(names.contains("ai_complete"), "missing ai_complete");
        assertTrue(names.contains("ai_summarize"), "missing ai_summarize");
        assertTrue(names.contains("ai_classify"), "missing ai_classify");
        assertTrue(names.contains("ai_filter"), "missing ai_filter");
        assertTrue(names.contains("ai_embed"), "missing ai_embed");
        assertTrue(names.contains("ai_extract"), "missing ai_extract");
        assertTrue(names.contains("ai_guard"), "missing ai_guard");
        assertTrue(names.contains("ai_predict"), "missing ai_predict");
        assertTrue(names.contains("ai_translate"), "missing ai_translate");
        assertTrue(names.contains(
                AiRerankScoreFunction.NAME), "missing governed rerank score");
        assertTrue(names.contains("file_from_url"));
        assertTrue(names.contains("file_from_bytes"));
    }

    @Test
    void aiCompleteReturnsGeneratedText() {
        var fn = new AiCompletionFunction(name -> stubProvider);
        stubProvider.setResponse("Hello world");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("Say hello")));
        assertInstanceOf(ColumnValue.StringValue.class, result);
        assertEquals("Hello world", ((ColumnValue.StringValue) result).value());
    }

    @Test
    void factoryFunctionRequiresAndConsumesExactNativeBinding() {
        ScalarFunction function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name()
                        .equals("ai_complete"))
                .findFirst()
                .orElseThrow();
        stubProvider.setResponse("bound answer");

        ColumnValue result = function.invoke(governedArguments(
                org.pangrova.protocol.BuiltInCapabilityCatalog
                        .aiTextGenerationPlanRef()));

        assertEquals(
                "bound answer", ((ColumnValue.StringValue) result).value());
    }

    @Test
    void factoryFunctionRejectsCapabilitySignatureTampering() {
        ScalarFunction function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name()
                        .equals("ai_complete"))
                .findFirst()
                .orElseThrow();
        var tampered = org.pangrova.protocol.BuiltInCapabilityCatalog
                .aiTextGenerationPlanRef().toBuilder()
                .setSignatureSha256(com.google.protobuf.ByteString.copyFrom(
                        new byte[32]))
                .build();

        assertThrows(IllegalArgumentException.class, () ->
                function.invoke(governedArguments(tampered)));
    }

    @Test
    void factoryFunctionRejectsMixedBatchResourceBindings() {
        ScalarFunction function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name()
                        .equals("ai_complete"))
                .findFirst()
                .orElseThrow();
        List<ColumnValue> first = governedArguments(
                org.pangrova.protocol.BuiltInCapabilityCatalog
                        .aiTextGenerationPlanRef());
        List<ColumnValue> changed = new java.util.ArrayList<>(first);
        changed.set(10, new ColumnValue.StringValue("1"));

        assertThrows(IllegalArgumentException.class, () ->
                ((org.pangrova.spi.function.BatchableScalarFunction) function)
                        .invokeBatch(List.of(first, changed)));
    }

    @Test
    void aiCompleteReturnsNullOnNullInput() {
        var fn = new AiCompletionFunction(name -> stubProvider);

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.NullValue()));
        assertInstanceOf(ColumnValue.NullValue.class, result);
    }

    @Test
    void aiClassifyReturnsLabel() {
        var fn = new AiClassifyFunction(name -> stubProvider);
        stubProvider.setResponse("positive");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("This product is great!"),
                new ColumnValue.StringValue("positive,negative,neutral")));
        assertInstanceOf(ColumnValue.StringValue.class, result);
        assertEquals("positive", ((ColumnValue.StringValue) result).value());
    }

    @Test
    void aiSummarizeReturnsText() {
        var fn = new AiSummarizeFunction(name -> stubProvider);
        stubProvider.setResponse("Summary of the text.");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("A very long article...")));
        assertInstanceOf(ColumnValue.StringValue.class, result);
        assertEquals("Summary of the text.", ((ColumnValue.StringValue) result).value());
    }

    @Test
    void aiFilterReturnsBooleanTrue() {
        var fn = new AiFilterFunction(name -> stubProvider);
        stubProvider.setResponse("true");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("The product broke after 2 days"),
                new ColumnValue.StringValue("mentions product defect")));
        assertInstanceOf(ColumnValue.BooleanValue.class, result);
        assertTrue(((ColumnValue.BooleanValue) result).value());
    }

    @Test
    void aiFilterReturnsBooleanFalse() {
        var fn = new AiFilterFunction(name -> stubProvider);
        stubProvider.setResponse("false");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("Great product!"),
                new ColumnValue.StringValue("mentions product defect")));
        assertInstanceOf(ColumnValue.BooleanValue.class, result);
        assertFalse(((ColumnValue.BooleanValue) result).value());
    }

    @Test
    void missingProviderThrows() {
        var fn = new AiCompletionFunction(name -> null);

        assertThrows(IllegalStateException.class, () ->
                fn.invoke(List.of(
                        new ColumnValue.StringValue("unknown"),
                        new ColumnValue.StringValue("prompt"))));
    }

    @Test
    void factoryNameIsCorrect() {
        assertEquals("ai-inference", factory.getName());
    }

    private static List<ColumnValue> governedArguments(
            org.pangrova.protocol.v2.CapabilityRef capability) {
        String base64ModelSignature = java.util.Base64.getEncoder()
                .encodeToString(new byte[32]);
        return List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("Say hello"),
                new ColumnValue.StringValue("ai-invoke-1"),
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("v1"),
                new ColumnValue.StringValue(base64ModelSignature),
                new ColumnValue.StringValue(capability.getCapabilityId()),
                new ColumnValue.StringValue(capability.getSemanticVersion()),
                new ColumnValue.StringValue(java.util.Base64.getEncoder()
                        .encodeToString(
                                capability.getSignatureSha256().toByteArray())),
                new ColumnValue.StringValue(
                        "query-1:stage-1:task-1:ai-invoke-1"),
                new ColumnValue.StringValue("2"),
                new ColumnValue.StringValue("1024"),
                new ColumnValue.StringValue("1024000"),
                new ColumnValue.StringValue("512000"),
                new ColumnValue.StringValue("1000000"),
                new ColumnValue.StringValue("false"),
                new ColumnValue.StringValue("0"),
                new ColumnValue.StringValue("0"),
                new ColumnValue.StringValue("0"),
                new ColumnValue.StringValue("0.0"),
                new ColumnValue.StringValue("0"),
                new ColumnValue.StringValue("false"),
                new ColumnValue.StringValue("0"));
    }

    /**
     * Simple stub inference provider for testing.
     */
    private static class StubInferenceProvider implements InferenceProvider {
        private String response = "";

        void setResponse(String response) {
            this.response = response;
        }

        @Override
        public InferenceResult complete(String prompt, GenerationParameters parameters) {
            return InferenceResult.of(response);
        }

        @Override
        public List<InferenceResult> completeBatch(List<String> prompts, GenerationParameters parameters) {
            return prompts.stream().map(p -> InferenceResult.of(response)).toList();
        }

        @Override
        public String modelName() {
            return "test-model";
        }

        @Override
        public int maxBatchSize() {
            return 10;
        }
    }
}
