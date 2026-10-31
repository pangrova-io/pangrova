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

import org.pangrova.ai.worker.function.file.FileStructHelper;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.InferenceProvider;
import org.pangrova.ai.api.invocation.InferenceResult;
import org.pangrova.ai.api.capability.Modality;
import org.pangrova.ai.api.invocation.MultimodalPrompt;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.function.api.scalar.ScalarFunction;

import java.util.List;
import java.util.function.Function;

/**
 * Scalar SQL function: {@code ai_ocr(model, FILE) -> VARCHAR}.
 *
 * <p>Performs Optical Character Recognition (OCR) on an image or scanned
 * document FILE, extracting all visible text content. Uses the default
 * configured vision model.
 *
 * <p>Example:
 * <pre>
 *   SELECT ai_ocr('gpt-4o', scanned_doc) FROM receipts
 * </pre>
 *
 * <p>Thread-safe: delegates to thread-safe InferenceProvider.</p>
 */
public final class AiOcrFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("ai_ocr");

    private static final String OCR_PROMPT =
            "Perform OCR on this image. Extract ALL visible text exactly as it appears, "
                    + "preserving layout and structure. Return only the extracted text, nothing else.";

    private final Function<String, InferenceProvider> providerLookup;

    /**
     * Creates an OCR function with the specified default vision model.
     *
     * @param providerLookup   provider lookup function
     * @param defaultVisionModel the default model name for OCR (e.g., "gpt-4o")
     */
    AiOcrFunction(Function<String, InferenceProvider> providerLookup) {
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
        if (arguments.size() != 2) {
            throw new IllegalArgumentException("ai_ocr requires 2 arguments: model, file");
        }
        String modelName = extractString(arguments.get(0), "model");
        ColumnValue fileArg = arguments.get(1);
        if (modelName == null) {
            return new ColumnValue.NullValue();
        }
        if (fileArg instanceof ColumnValue.NullValue) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException(
                    "Vision model not found: " + modelName);
        }

        // Verify vision capability — silent text-only fallback would produce incorrect results
        if (!provider.supportedModalities().contains(Modality.IMAGE)) {
            throw new IllegalStateException(
                    "Vision model '" + modelName + "' does not support IMAGE modality. "
                            + "Supported modalities: " + provider.supportedModalities());
        }

        MultimodalPrompt prompt = FileStructHelper.buildMultimodalPrompt(OCR_PROMPT, fileArg);
        InferenceResult result = provider.completeMultimodal(prompt, GenerationParameters.DEFAULTS);
        return new ColumnValue.StringValue(result.text());
    }

    private static String extractString(ColumnValue value, String name) {
        if (value instanceof ColumnValue.NullValue) {
            return null;
        }
        if (value instanceof ColumnValue.StringValue string) {
            return string.value();
        }
        throw new IllegalArgumentException("ai_ocr: " + name + " must be VARCHAR");
    }
}
