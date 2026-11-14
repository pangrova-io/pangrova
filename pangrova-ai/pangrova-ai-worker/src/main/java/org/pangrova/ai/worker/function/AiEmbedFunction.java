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
package org.pangrova.ai.worker.function;

import org.pangrova.ai.worker.function.file.FileStruct;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.embedding.EmbeddingResultValidator;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.ai.api.embedding.EmbeddingBatchRequest;
import org.pangrova.ai.api.enums.EmbeddingPurpose;
import org.pangrova.ai.api.embedding.EmbeddingProvider;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.function.api.scalar.BatchableScalarFunction;
import org.pangrova.function.api.metadata.FunctionMetadata;

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
 * multimodal embedding. Returns a first-class dense vector.
 *
 * <p>FILE input requires a provider that advertises a non-TEXT modality; the bundled
 * OpenAI-compatible provider is text-only, so {@code ai_embed(model, photo)} raises
 * CAPABILITY_MISMATCH until a multimodal provider is configured. Example:
 *
 * <pre>
 *   SELECT ai_embed('text-embedding-3-small', title) FROM articles
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
            var reference = FileStruct.decodeReference(input);
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
            embedChunk(key, provider, rows.subList(offset, end), results);
            offset = end;
        }
    }

    /**
     * Embeds one provider-sized chunk. A provider failure is narrowed by bisection
     * until the offending row is isolated, so the error names that row instead of
     * failing the whole SQL batch anonymously. No value is ever synthesized.
     */
    private void embedChunk(BatchKey key, EmbeddingProvider provider,
                            List<IndexedInput> chunk, ColumnValue[] results) {
        try {
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
        } catch (AiInvocationException failure) {
            if (chunk.size() == 1) {
                throw rowFailure(failure, key, chunk.get(0));
            }
            int middle = chunk.size() / 2;
            embedChunk(key, provider, chunk.subList(0, middle), results);
            embedChunk(key, provider, chunk.subList(middle, chunk.size()), results);
        }
    }

    private static AiInvocationException rowFailure(
            AiInvocationException failure, BatchKey key, IndexedInput row) {
        return new AiInvocationException(failure.category(), failure.retryable(),
                "ai_embed failed for batch row " + row.index() + " (model=" + key.model()
                        + ", input=" + describe(row.input()) + "): " + failure.getMessage(),
                failure);
    }

    private static String describe(EmbeddingBatchRequest.Input input) {
        if (input instanceof EmbeddingBatchRequest.TextInput text) {
            String value = text.text();
            return value.length() <= 64 ? value : value.substring(0, 64) + "...";
        }
        if (input instanceof EmbeddingBatchRequest.BlobInput blob) {
            return blob.reference().uri() + " checksum=" + blob.reference().checksum();
        }
        return input.getClass().getSimpleName();
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
