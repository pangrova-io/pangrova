package org.pangrova.worker.ai.function;

import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.ai.inference.InferenceResult;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.BatchableScalarFunction;
import org.pangrova.spi.function.FunctionMetadata;
import org.pangrova.protocol.function.FunctionBindingDeclarations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scalar SQL function: {@code ai_classify(model, text, labels) -> VARCHAR}.
 *
 * <p>Classifies input text into one of the provided labels. Example:
 * <pre>
 *   SELECT ai_classify('gpt-4o', review, 'positive,negative,neutral') FROM reviews
 * </pre>
 */
public final class AiClassifyFunction implements BatchableScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("ai_classify");

    private final java.util.function.Function<String, InferenceProvider> providerLookup;

    /**
     * Creates a classify function backed by the given provider lookup.
     *
     * @param providerLookup resolves model names to inference providers
     */
    AiClassifyFunction(
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
        if (arguments.size() < 3) {
            throw new IllegalArgumentException(
                    "ai_classify requires 3 arguments: model, text, labels");
        }
        String modelName = extractString(arguments.get(0), "model");
        String text = extractString(arguments.get(1), "text");
        String labels = extractString(arguments.get(2), "labels");
        if (modelName == null || text == null || labels == null) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }

        String prompt = "Classify the following text into exactly one of these labels: ["
                + labels + "]. Respond with only the label, nothing else.\n\nText: " + text;
        String result = provider.complete(prompt, GenerationParameters.deterministic()).text();
        return new ColumnValue.StringValue(result.strip());
    }

    @Override
    public List<ColumnValue> invokeBatch(List<List<ColumnValue>> argumentsBatch) {
        Map<String, List<Integer>> modelToIndices = new HashMap<>();
        String[] texts = new String[argumentsBatch.size()];
        String[] labels = new String[argumentsBatch.size()];
        String[] models = new String[argumentsBatch.size()];

        for (int i = 0; i < argumentsBatch.size(); i++) {
            List<ColumnValue> args = argumentsBatch.get(i);
            if (args.size() < 3) {
                throw new IllegalArgumentException("ai_classify requires 3 arguments: model, text, labels");
            }
            models[i] = extractString(args.get(0), "model");
            texts[i] = extractString(args.get(1), "text");
            labels[i] = extractString(args.get(2), "labels");
            if (models[i] != null && texts[i] != null && labels[i] != null) {
                modelToIndices.computeIfAbsent(models[i], k -> new ArrayList<>()).add(i);
            }
        }

        ColumnValue[] results = new ColumnValue[argumentsBatch.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = new ColumnValue.NullValue();
        }

        for (Map.Entry<String, List<Integer>> entry : modelToIndices.entrySet()) {
            classifyForModel(entry.getKey(), entry.getValue(), texts, labels, results);
        }
        return List.of(results);
    }

    private void classifyForModel(String modelName, List<Integer> indices, String[] texts,
                                  String[] labels, ColumnValue[] results) {
        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }
        List<String> batchPrompts = indices.stream()
                .map(i -> "Classify the following text into exactly one of these labels: ["
                        + labels[i] + "]. Respond with only the label, nothing else.\n\nText: " + texts[i])
                .toList();
        List<InferenceResult> completions = provider.completeBatch(batchPrompts,
                GenerationParameters.deterministic());
        for (int j = 0; j < indices.size(); j++) {
            results[indices.get(j)] = new ColumnValue.StringValue(completions.get(j).text().strip());
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
                "ai_classify: " + argName + " must be VARCHAR");
    }
}
