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

import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.connector.data.ColumnValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AiEmbedFunctionTest {

    private final float[] testVector = {0.1f, 0.2f, 0.3f};

    private final EmbeddingProvider fixedProvider = new EmbeddingProvider() {
        @Override
        public float[] embed(String text) {
            return testVector.clone();
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            return texts.stream().map(t -> testVector.clone()).toList();
        }

        @Override
        public List<float[]> embedBlob(List<BlobReference> refs) {
            return refs.stream().map(r -> testVector.clone()).toList();
        }

        @Override
        public int dimensions() {
            return 3;
        }

        @Override
        public String modelName() {
            return "test-embed-model";
        }

        @Override
        public int maxBatchSize() {
            return 32;
        }
    };

    private final AiEmbedFunction fn = new AiEmbedFunction(
            name -> "test-embed-model".equals(name) ? fixedProvider : null);

    @Test
    void embedReturnsJsonFloatArray() {
        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-embed-model"),
                new ColumnValue.StringValue("hello world")));

        assertInstanceOf(ColumnValue.StringValue.class, result);
        String json = ((ColumnValue.StringValue) result).value();
        assertEquals("[0.1,0.2,0.3]", json);
    }

    @Test
    void embedReturnsNullOnNullText() {
        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-embed-model"),
                new ColumnValue.NullValue()));

        assertInstanceOf(ColumnValue.NullValue.class, result);
    }

    @Test
    void embedReturnsNullOnNullModel() {
        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.NullValue(),
                new ColumnValue.StringValue("some text")));

        assertInstanceOf(ColumnValue.NullValue.class, result);
    }

    @Test
    void embedThrowsOnUnknownModel() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                fn.invoke(List.of(
                        new ColumnValue.StringValue("unknown-model"),
                        new ColumnValue.StringValue("hello"))));

        assertTrue(ex.getMessage().contains("unknown-model"));
    }

    @Test
    void embedThrowsOnTooFewArguments() {
        assertThrows(IllegalArgumentException.class, () ->
                fn.invoke(List.of(
                        new ColumnValue.StringValue("test-embed-model"))));
    }

    @Test
    void floatsToJsonProducesCompactFormat() {
        assertEquals("[1.5,-0.3,0.0]",
                AiEmbedFunction.floatsToJson(new float[]{1.5f, -0.3f, 0.0f}));
    }

    @Test
    void floatsToJsonHandlesEmptyArray() {
        assertEquals("[]", AiEmbedFunction.floatsToJson(new float[0]));
    }

    @Test
    void floatsToJsonHandlesSingleElement() {
        assertEquals("[0.42]", AiEmbedFunction.floatsToJson(new float[]{0.42f}));
    }
}
