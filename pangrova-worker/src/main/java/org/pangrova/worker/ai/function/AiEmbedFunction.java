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
import org.pangrova.spi.function.BatchableScalarFunction;
import org.pangrova.spi.function.FunctionMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Scalar SQL function: {@code ai_embed(model, text_or_file) -> VARCHAR}.
 *
 * <p>Generates an embedding vector for the input text or FILE via
 * {@link EmbeddingProvider}. When the second argument is a ROW (FILE struct),
 * delegates to {@link EmbeddingProvider#embedBlob(List)} for multimodal embedding.
 * Returns a JSON array of floats. Example:
 * <pre>
 *   SELECT ai_embed('text-embedding-3-small', title) FROM articles
 *   SELECT ai_embed('clip-vit-large', photo) FROM products
 * </pre>
 *
 * <p>Thread-safe: delegates to thread-safe EmbeddingProvider.</p>
 */
public final class AiEmbedFunction implements BatchableScalarFunction {

    private static final FunctionMetadata METADATA = new FunctionMetadata(
            "ai_embed",
            "VARCHAR",
            List.of("VARCHAR", "VARCHAR"),
            false,
            "Generates an embedding vector for the input text or FILE");

    private final Function<String, EmbeddingProvider> embeddingLookup;

    /**
     * Creates an embed function backed by the given embedding provider lookup.
     *
     * @param embeddingLookup resolves model names to embedding providers
     */
    public AiEmbedFunction(Function<String, EmbeddingProvider> embeddingLookup) {
        if (embeddingLookup == null) {
            throw new IllegalArgumentException("embeddingLookup required");
        }
        this.embeddingLookup = embeddingLookup;
    }

    @Override
    public FunctionMetadata getMetadata() {
        return METADATA;
    }

    @Override
    public ColumnValue invoke(List<ColumnValue> arguments) {
        if (arguments.size() < 2) {
            throw new IllegalArgumentException(
                    "ai_embed requires 2 arguments: model, text_or_file");
        }
        String modelName = extractString(arguments.get(0), "model");
        if (modelName == null) {
            return new ColumnValue.NullValue();
        }

        ColumnValue inputArg = arguments.get(1);
        if (inputArg instanceof ColumnValue.NullValue) {
            return new ColumnValue.NullValue();
        }

        EmbeddingProvider provider = embeddingLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException(
                    "No EmbeddingProvider found for model: " + modelName);
        }

        // Handle FILE (ROW) input — multimodal blob embedding
        if (inputArg instanceof ColumnValue.RowValue) {
            BlobReference ref = FileStructHelper.extractBlobReference(inputArg);
            if (ref == null) {
                return new ColumnValue.NullValue();
            }
            List<float[]> results = provider.embedBlob(List.of(ref));
            return new ColumnValue.StringValue(floatsToJson(results.get(0)));
        }

        // Handle text input
        String text = extractString(inputArg, "text");
        if (text == null) {
            return new ColumnValue.NullValue();
        }

        float[] embedding = provider.embed(text);
        return new ColumnValue.StringValue(floatsToJson(embedding));
    }

    @Override
    public List<ColumnValue> invokeBatch(List<List<ColumnValue>> argumentsBatch) {
        // Group by model for efficient batching
        Map<String, List<Integer>> modelToIndices = new HashMap<>();
        String[] texts = new String[argumentsBatch.size()];
        String[] models = new String[argumentsBatch.size()];

        for (int i = 0; i < argumentsBatch.size(); i++) {
            List<ColumnValue> args = argumentsBatch.get(i);
            if (args.size() < 2) {
                throw new IllegalArgumentException("ai_embed requires 2 arguments: model, text_or_file");
            }
            models[i] = extractString(args.get(0), "model");
            // For batch, only handle text inputs; FILE inputs fall through to per-row invoke
            texts[i] = batchTextFor(args.get(1));
            if (models[i] != null && texts[i] != null) {
                modelToIndices.computeIfAbsent(models[i], k -> new ArrayList<>()).add(i);
            }
        }

        ColumnValue[] results = new ColumnValue[argumentsBatch.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = new ColumnValue.NullValue();
        }

        embedFileRows(argumentsBatch, results);

        // Handle text inputs in batch
        for (Map.Entry<String, List<Integer>> entry : modelToIndices.entrySet()) {
            embedTextsForModel(entry.getKey(), entry.getValue(), texts, results);
        }
        return List.of(results);
    }

    /**
     * Returns the text input for batch embedding, or null for FILE/NULL inputs.
     */
    private static String batchTextFor(ColumnValue inputArg) {
        if (inputArg instanceof ColumnValue.StringValue sv) {
            return sv.value();
        }
        return null;
    }

    private void embedFileRows(List<List<ColumnValue>> argumentsBatch, ColumnValue[] results) {
        // Handle FILE inputs per-row
        for (int i = 0; i < argumentsBatch.size(); i++) {
            List<ColumnValue> args = argumentsBatch.get(i);
            if (args.size() >= 2 && args.get(1) instanceof ColumnValue.RowValue) {
                results[i] = invoke(args);
            }
        }
    }

    private void embedTextsForModel(String modelName, List<Integer> indices, String[] texts,
                                    ColumnValue[] results) {
        EmbeddingProvider provider = embeddingLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException(
                    "No EmbeddingProvider found for model: " + modelName);
        }
        List<String> batchTexts = indices.stream().map(i -> texts[i]).toList();
        List<float[]> embeddings = provider.embedBatch(batchTexts);
        for (int j = 0; j < indices.size(); j++) {
            results[indices.get(j)] = new ColumnValue.StringValue(
                    floatsToJson(embeddings.get(j)));
        }
    }

    static String floatsToJson(float[] values) {
        StringBuilder sb = new StringBuilder(values.length * 8 + 2);
        sb.append('[');
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(values[i]);
        }
        sb.append(']');
        return sb.toString();
    }

    private static String extractString(ColumnValue cv, String argName) {
        if (cv instanceof ColumnValue.StringValue sv) {
            return sv.value();
        }
        if (cv instanceof ColumnValue.NullValue) {
            return null;
        }
        throw new IllegalArgumentException(
                "ai_embed: " + argName + " must be VARCHAR");
    }
}
