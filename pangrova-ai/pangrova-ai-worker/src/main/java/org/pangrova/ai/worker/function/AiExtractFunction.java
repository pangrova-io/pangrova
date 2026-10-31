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

import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.InferenceProvider;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.function.api.scalar.ScalarFunction;

import java.util.List;

/**
 * Scalar SQL function: {@code ai_extract(model, text, schema) -> VARCHAR}.
 *
 * <p>Extracts structured data from text according to a schema, returning valid
 * JSON. Example:
 * <pre>
 *   SELECT ai_extract('gpt-4o', body, '{"name": "string", "age": "int"}') FROM docs
 * </pre>
 *
 * <p>Thread-safe: delegates to thread-safe InferenceProvider.</p>
 */
public final class AiExtractFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("ai_extract");

    private final java.util.function.Function<String, InferenceProvider> providerLookup;

    /**
     * Creates an extract function backed by the given provider lookup.
     *
     * @param providerLookup resolves model names to inference providers
     */
    AiExtractFunction(
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
                    "ai_extract requires 3 arguments: model, text, schema");
        }
        String modelName = extractString(arguments.get(0), "model");
        String text = extractString(arguments.get(1), "text");
        String schema = extractString(arguments.get(2), "schema");
        if (modelName == null || text == null || schema == null) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }

        String prompt = "Extract the following fields from the text according to this schema: "
                + schema + ". Return valid JSON.\n\nText: " + text;
        String result = provider.complete(prompt, GenerationParameters.deterministicJson()).text();
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
                "ai_extract: " + argName + " must be VARCHAR");
    }
}
