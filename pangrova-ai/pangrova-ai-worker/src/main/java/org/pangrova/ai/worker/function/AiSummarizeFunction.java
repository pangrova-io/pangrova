package org.pangrova.ai.worker.function;

import org.pangrova.spi.ai.invocation.GenerationParameters;
import org.pangrova.spi.ai.invocation.InferenceProvider;
import org.pangrova.spi.ai.invocation.InferenceResult;
import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.function.scalar.BatchableScalarFunction;
import org.pangrova.spi.function.metadata.FunctionMetadata;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scalar SQL function: {@code ai_summarize(model, text) -> VARCHAR}.
 *
 * <p>Produces a concise summary of the input text. Example:
 * <pre>
 *   SELECT ai_summarize('gpt-4o', body) FROM articles
 * </pre>
 */
public final class AiSummarizeFunction implements BatchableScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("ai_summarize");

    private final java.util.function.Function<String, InferenceProvider> providerLookup;

    /**
     * Creates a summarize function backed by the given provider lookup.
     *
     * @param providerLookup resolves model names to inference providers
     */
    AiSummarizeFunction(
            java.util.function.Function<String, InferenceProvider> providerLookup) {
        if (providerLookup == null) {
            throw new IllegalArgumentException("providerLookup required");
        }
        this.providerLookup = providerLookup;
    }

    @Override
    public FunctionMetadata getMetadata() {
        return METADATA;
    }

    @Override
    public ColumnValue invoke(List<ColumnValue> arguments) {
        if (arguments.size() < 2) {
            throw new IllegalArgumentException(
                    "ai_summarize requires 2 arguments: model, text");
        }
        String modelName = extractString(arguments.get(0), "model");
        String text = extractString(arguments.get(1), "text");
        if (modelName == null || text == null) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }

        String prompt = "Summarize the following text concisely:\n\n" + text;
        String result = provider.complete(prompt, GenerationParameters.DEFAULTS).text();
        return new ColumnValue.StringValue(result);
    }

    @Override
    public List<ColumnValue> invokeBatch(List<List<ColumnValue>> argumentsBatch) {
        Map<String, List<Integer>> modelToIndices = new HashMap<>();
        String[] texts = new String[argumentsBatch.size()];
        String[] models = new String[argumentsBatch.size()];

        for (int i = 0; i < argumentsBatch.size(); i++) {
            List<ColumnValue> args = argumentsBatch.get(i);
            if (args.size() < 2) {
                throw new IllegalArgumentException("ai_summarize requires 2 arguments: model, text");
            }
            models[i] = extractString(args.get(0), "model");
            texts[i] = extractString(args.get(1), "text");
            if (models[i] != null && texts[i] != null) {
                modelToIndices.computeIfAbsent(models[i], k -> new ArrayList<>()).add(i);
            }
        }

        ColumnValue[] results = new ColumnValue[argumentsBatch.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = new ColumnValue.NullValue();
        }

        for (Map.Entry<String, List<Integer>> entry : modelToIndices.entrySet()) {
            summarizeForModel(entry.getKey(), entry.getValue(), texts, results);
        }
        return List.of(results);
    }

    private void summarizeForModel(String modelName, List<Integer> indices, String[] texts,
                                   ColumnValue[] results) {
        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }
        List<String> batchPrompts = indices.stream()
                .map(i -> "Summarize the following text concisely:\n\n" + texts[i])
                .toList();
        List<InferenceResult> completions = provider.completeBatch(batchPrompts, GenerationParameters.DEFAULTS);
        for (int j = 0; j < indices.size(); j++) {
            results[indices.get(j)] = new ColumnValue.StringValue(completions.get(j).text());
        }
    }

    private static String extractString(ColumnValue cv, String argName) {
        if (cv instanceof ColumnValue.StringValue sv) {
            return sv.value();
        }
        if (cv instanceof ColumnValue.NullValue) {
            return null;
        }
        throw new IllegalArgumentException(
                "ai_summarize: " + argName + " must be VARCHAR");
    }
}
