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
import org.pangrova.ai.api.inference.InferenceProvider;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.function.api.scalar.ScalarFunction;

import java.util.List;

/**
 * Scalar SQL function: {@code ai_translate(model, text, target_language) -> VARCHAR}.
 *
 * <p>Translates text to the specified target language. Example:
 * <pre>
 *   SELECT ai_translate('gpt-4o', body, 'Spanish') FROM articles
 * </pre>
 *
 * <p>Thread-safe: delegates to thread-safe InferenceProvider.</p>
 */
public final class AiTranslateFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("ai_translate");

    private final java.util.function.Function<String, InferenceProvider> providerLookup;

    /**
     * Creates a translate function backed by the given provider lookup.
     *
     * @param providerLookup resolves model names to inference providers
     */
    AiTranslateFunction(
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
                    "ai_translate requires 3 arguments: model, text, target_language");
        }
        String modelName = extractString(arguments.get(0), "model");
        String text = extractString(arguments.get(1), "text");
        String targetLanguage = extractString(arguments.get(2), "target_language");
        if (modelName == null || text == null || targetLanguage == null) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }

        String prompt = "Translate the following text to " + targetLanguage
                + ". Respond with only the translation, nothing else.\n\nText: " + text;
        String result = provider.complete(prompt, GenerationParameters.deterministic()).text();
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
                "ai_translate: " + argName + " must be VARCHAR");
    }
}
