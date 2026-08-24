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

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AiFunctionFactoryTest {

    private StubInferenceProvider stubProvider;
    private AiFunctionFactory factory;

    @BeforeEach
    void setUp() {
        stubProvider = new StubInferenceProvider();
        factory = new AiFunctionFactory(
                name -> "test-model".equals(name) ? stubProvider : null,
                name -> null);
    }

    @Test
    void factoryProvidesAllFourFunctions() {
        List<ScalarFunction> functions = factory.getScalarFunctions(Map.of());
        assertEquals(15, functions.size());
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
