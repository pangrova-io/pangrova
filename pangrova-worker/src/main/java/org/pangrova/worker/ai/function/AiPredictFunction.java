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
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.FunctionMetadata;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.spi.function.ScalarFunction;

import java.util.List;

/**
 * Scalar SQL function: {@code ai_predict(model, features) -> VARCHAR}.
 *
 * <p>Generates a prediction based on input features using the specified model.
 * Example:
 * <pre>
 *   SELECT ai_predict('gpt-4o', features) FROM dataset
 * </pre>
 *
 * <p>Thread-safe: delegates to thread-safe InferenceProvider.</p>
 */
public final class AiPredictFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("ai_predict");

    private final java.util.function.Function<String, InferenceProvider> providerLookup;

    /**
     * Creates a predict function backed by the given provider lookup.
     *
     * @param providerLookup resolves model names to inference providers
     */
    AiPredictFunction(
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
                    "ai_predict requires 2 arguments: model, features");
        }
        String modelName = extractString(arguments.get(0), "model");
        String features = extractString(arguments.get(1), "features");
        if (modelName == null || features == null) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }

        String prompt = "Based on these features, make a prediction: " + features;
        String result = provider.complete(prompt, GenerationParameters.DEFAULTS).text();
        return new ColumnValue.StringValue(result);
    }

    private static String extractString(ColumnValue cv, String argName) {
        if (cv instanceof ColumnValue.StringValue sv) {
            return sv.value();
        }
        if (cv instanceof ColumnValue.NullValue) {
            return null;
        }
        throw new IllegalArgumentException(
                "ai_predict: " + argName + " must be VARCHAR");
    }
}
