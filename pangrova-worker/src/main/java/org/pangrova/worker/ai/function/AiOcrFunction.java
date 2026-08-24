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
import org.pangrova.spi.ai.inference.InferenceResult;
import org.pangrova.spi.ai.inference.Modality;
import org.pangrova.spi.ai.inference.MultimodalPrompt;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.FunctionMetadata;
import org.pangrova.spi.function.ScalarFunction;

import java.util.List;
import java.util.function.Function;

/**
 * Scalar SQL function: {@code ai_ocr(FILE) -> VARCHAR}.
 *
 * <p>Performs Optical Character Recognition (OCR) on an image or scanned
 * document FILE, extracting all visible text content. Uses the default
 * configured vision model.
 *
 * <p>Example:
 * <pre>
 *   SELECT ai_ocr(scanned_doc) FROM receipts
 * </pre>
 *
 * <p>Unlike other AI functions, ai_ocr does not require a model name argument —
 * it uses the system's default vision-capable model.
 *
 * <p>Thread-safe: delegates to thread-safe InferenceProvider.</p>
 */
public final class AiOcrFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA = new FunctionMetadata(
            "ai_ocr",
            "VARCHAR",
            List.of("ROW"),
            false,
            "Extracts text from an image or scanned document FILE via OCR");

    private static final String OCR_PROMPT =
            "Perform OCR on this image. Extract ALL visible text exactly as it appears, "
                    + "preserving layout and structure. Return only the extracted text, nothing else.";

    private final Function<String, InferenceProvider> providerLookup;
    private final String defaultVisionModel;

    /**
     * Creates an OCR function with the specified default vision model.
     *
     * @param providerLookup   provider lookup function
     * @param defaultVisionModel the default model name for OCR (e.g., "gpt-4o")
     */
    public AiOcrFunction(Function<String, InferenceProvider> providerLookup,
                          String defaultVisionModel) {
        if (providerLookup == null) {
            throw new IllegalArgumentException("providerLookup required");
        }
        this.providerLookup = providerLookup;
        this.defaultVisionModel = defaultVisionModel != null ? defaultVisionModel : "gpt-4o";
    }

    public AiOcrFunction(Function<String, InferenceProvider> providerLookup) {
        this(providerLookup, "gpt-4o");
    }

    @Override
    public FunctionMetadata getMetadata() {
        return METADATA;
    }

    @Override
    public ColumnValue invoke(List<ColumnValue> arguments) {
        if (arguments.isEmpty()) {
            throw new IllegalArgumentException("ai_ocr requires 1 argument: file");
        }

        ColumnValue fileArg = arguments.get(0);
        if (fileArg instanceof ColumnValue.NullValue) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(defaultVisionModel);
        if (provider == null) {
            throw new IllegalStateException(
                    "Default vision model not found: " + defaultVisionModel);
        }

        // Verify vision capability — silent text-only fallback would produce incorrect results
        if (!provider.supportedModalities().contains(Modality.IMAGE)) {
            throw new IllegalStateException(
                    "Default vision model '" + defaultVisionModel + "' does not support IMAGE modality. "
                            + "Supported modalities: " + provider.supportedModalities());
        }

        MultimodalPrompt prompt = FileStructHelper.buildMultimodalPrompt(OCR_PROMPT, fileArg);
        InferenceResult result = provider.completeMultimodal(prompt, GenerationParameters.DEFAULTS);
        return new ColumnValue.StringValue(result.text());
    }
}
