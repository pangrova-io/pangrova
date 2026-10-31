package org.pangrova.ai.worker.function;

import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.InferenceProvider;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.function.api.scalar.ScalarFunction;

import java.util.List;
import java.util.Set;

/**
 * Scalar SQL function: {@code ai_filter(model, text, criteria) -> BOOLEAN}.
 *
 * <p>Returns true if the text matches the specified criteria using LLM reasoning.
 * Example:
 * <pre>
 *   SELECT * FROM reviews WHERE ai_filter('gpt-4o', body, 'mentions product defect')
 * </pre>
 */
public final class AiFilterFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("ai_filter");

    private static final Set<String> TRUE_TOKENS = Set.of("true", "yes", "1");

    private final java.util.function.Function<String, InferenceProvider> providerLookup;

    /**
     * Creates a filter function backed by the given provider lookup.
     *
     * @param providerLookup resolves model names to inference providers
     */
    AiFilterFunction(
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
                    "ai_filter requires 3 arguments: model, text, criteria");
        }
        String modelName = extractString(arguments.get(0), "model");
        String text = extractString(arguments.get(1), "text");
        String criteria = extractString(arguments.get(2), "criteria");
        if (modelName == null || text == null || criteria == null) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }

        String prompt = "Does the following text match this criteria: \""
                + criteria + "\"? Answer only 'true' or 'false'.\n\nText: " + text;
        String result = provider.complete(prompt, GenerationParameters.deterministic()).text();
        boolean matches = TRUE_TOKENS.contains(result.strip().toLowerCase());
        return new ColumnValue.BooleanValue(matches);
    }

    private static String extractString(ColumnValue cv, String argName) {
        if (cv instanceof ColumnValue.StringValue sv) {
            return sv.value();
        }
        if (cv instanceof ColumnValue.NullValue) {
            return null;
        }
        throw new IllegalArgumentException(
                "ai_filter: " + argName + " must be VARCHAR");
    }
}
