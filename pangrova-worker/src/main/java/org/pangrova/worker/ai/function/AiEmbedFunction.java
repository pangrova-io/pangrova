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

import org.pangrova.worker.ai.function.file.FileStructHelper;
import org.pangrova.worker.ai.context.AiExecutionContext;
import org.pangrova.worker.ai.embedding.EmbeddingResultValidator;

import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import org.pangrova.spi.ai.embedding.EmbeddingBatchRequest;
import org.pangrova.spi.ai.embedding.EmbeddingPurpose;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.BatchableScalarFunction;
import org.pangrova.spi.function.FunctionMetadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Normalized SQL function: {@code ai_embed(model, text_or_file, purpose) -> VECTOR}.
 *
 * <p>Generates an embedding vector for the input text or FILE via {@link EmbeddingProvider}. When
 * the second argument is a ROW (FILE struct), uses the same ordered provider batch contract for
 * multimodal embedding. Returns a first-class dense vector. Example:
 *
 * <pre>
 *   SELECT ai_embed('text-embedding-3-small', title) FROM articles
 *   SELECT ai_embed('clip-vit-large', photo) FROM products
 * </pre>
 *
 * <p>Thread-safe: delegates to thread-safe EmbeddingProvider.
 */
public final class AiEmbedFunction implements BatchableScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("ai_embed");

    private final Function<String, EmbeddingProvider> embeddingLookup;

    /**
     * Creates an embed function backed by the given embedding provider lookup.
     *
     * @param embeddingLookup resolves model names to embedding providers
     */
    AiEmbedFunction(Function<String, EmbeddingProvider> embeddingLookup) {
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
    public List<List<String>> getArgumentSignatures() {
        return FunctionBindingDeclarations.callbackArgumentSignatures("ai_embed");
    }

    @Override
    public ColumnValue invoke(List<ColumnValue> arguments) {
        return invokeBatch(List.of(arguments)).get(0);
    }

    @Override
    public List<ColumnValue> invokeBatch(List<List<ColumnValue>> argumentsBatch) {
        Map<BatchKey, List<IndexedInput>> groups = prepareInputs(argumentsBatch);
        ColumnValue[] results = new ColumnValue[argumentsBatch.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = new ColumnValue.NullValue();
        }

        for (var group : groups.entrySet()) {
            embedGroup(group.getKey(), group.getValue(), results);
        }
        return List.of(results);
    }

    private static Map<BatchKey, List<IndexedInput>> prepareInputs(
            List<List<ColumnValue>> argumentsBatch) {
        Map<BatchKey, List<IndexedInput>> groups = new LinkedHashMap<>();
        for (int index = 0; index < argumentsBatch.size(); index++) {
            List<ColumnValue> arguments = argumentsBatch.get(index);
            if (arguments.size() != 3) {
                throw new IllegalArgumentException(
                        "ai_embed requires normalized model, input and purpose");
            }
            String purpose = extractString(arguments.get(2), "purpose");
            if (purpose == null) {
                throw new IllegalArgumentException("ai_embed purpose must not be NULL");
            }
            EmbeddingPurpose semanticPurpose = requirePurpose(purpose);
            String model = extractString(arguments.get(0), "model");
            EmbeddingBatchRequest.Input input = embeddingInput(arguments.get(1));
            if (model == null || input == null) {
                continue;
            }
            var key = new BatchKey(model, semanticPurpose, input.getClass());
            groups.computeIfAbsent(key, ignored -> new ArrayList<>())
                    .add(new IndexedInput(index, input));
        }
        return groups;
    }

    private static EmbeddingPurpose requirePurpose(String purpose) {
        try {
            return EmbeddingPurpose.valueOf(purpose);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "ai_embed purpose must be RAW, QUERY or DOCUMENT", exception);
        }
    }

    private static EmbeddingBatchRequest.Input embeddingInput(ColumnValue input) {
        if (input instanceof ColumnValue.NullValue) {
            return null;
        }
        if (input instanceof ColumnValue.StringValue text) {
            return new EmbeddingBatchRequest.TextInput(text.value());
        }
        if (input instanceof ColumnValue.RowValue) {
            var reference = FileStructHelper.extractBlobReference(input);
            return reference == null ? null : new EmbeddingBatchRequest.BlobInput(reference);
        }
        throw new IllegalArgumentException("ai_embed input must be VARCHAR or FILE");
    }

    private void embedGroup(BatchKey key, List<IndexedInput> rows, ColumnValue[] results) {
        EmbeddingProvider provider = embeddingLookup.apply(key.model());
        if (provider == null) {
            throw new IllegalStateException("No EmbeddingProvider found for model: " + key.model());
        }
        if (!provider.supportedPurposes().contains(key.purpose())) {
            throw new IllegalStateException(
                    "Embedding provider does not support purpose " + key.purpose());
        }
        requireExactOutputBinding(provider, key.model());
        int maxBatchSize = provider.maxBatchSize();
        for (int offset = 0; offset < rows.size(); ) {
            int end = offset + Math.min(maxBatchSize, rows.size() - offset);
            List<IndexedInput> chunk = rows.subList(offset, end);
            var batch =
                    new EmbeddingBatchRequest(
                            key.purpose(), chunk.stream().map(IndexedInput::input).toList());
            var response = provider.embed(batch);
            if (response == null) {
                throw new AiInvocationException(
                        AiInvocationException.Category.PROVIDER_RESPONSE_INVALID,
                        false,
                        "Embedding provider returned null batch result");
            }
            List<ColumnValue> embeddings =
                    EmbeddingResultValidator.validateBatch(
                                    provider, response.vectors(), chunk.size(), key.model())
                            .stream()
                            .map(ColumnValue.VectorValue::new)
                            .map(ColumnValue.class::cast)
                            .toList();
            for (int index = 0; index < chunk.size(); index++) {
                results[chunk.get(index).index()] = embeddings.get(index);
            }
            offset = end;
        }
    }

    private record BatchKey(String model, EmbeddingPurpose purpose, Class<?> inputRepresentation) {}

    private record IndexedInput(int index, EmbeddingBatchRequest.Input input) {}

    private static void requireExactOutputBinding(EmbeddingProvider provider, String modelName) {
        int expectedDimensions =
                EmbeddingResultValidator.requireProviderContract(provider, modelName);
        int boundDimension =
                AiExecutionContext.boundInvocation()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "ai_embed requires an exact model output binding"))
                        .outputVectorDimension();
        if (expectedDimensions != boundDimension) {
            throw new IllegalStateException(
                    "Embedding provider '"
                            + modelName
                            + "' dimension "
                            + expectedDimensions
                            + " differs from exact model binding "
                            + boundDimension);
        }
    }

    private static String extractString(ColumnValue cv, String argName) {
        if (cv instanceof ColumnValue.StringValue sv) {
            return sv.value();
        }
        if (cv instanceof ColumnValue.NullValue) {
            return null;
        }
        throw new IllegalArgumentException("ai_embed: " + argName + " must be VARCHAR");
    }
}
