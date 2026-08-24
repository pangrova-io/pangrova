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
 * Scalar SQL function: {@code ai_transcribe(model, FILE) -> VARCHAR}.
 *
 * <p>Transcribes audio content from a FILE column into text using a
 * speech-to-text model. Supports common audio formats (MP3, WAV, OGG, FLAC).
 *
 * <p>Example:
 * <pre>
 *   SELECT ai_transcribe('whisper-large-v3', recording) FROM call_logs
 * </pre>
 *
 * <p>Thread-safe: delegates to thread-safe InferenceProvider.</p>
 */
public final class AiTranscribeFunction implements ScalarFunction {

    private static final FunctionMetadata METADATA = new FunctionMetadata(
            "ai_transcribe",
            "VARCHAR",
            List.of("VARCHAR", "ROW"),
            false,
            "Transcribes audio FILE content to text using a speech-to-text model");

    private static final String DEFAULT_PROMPT =
            "Transcribe the audio content accurately. Return the full transcript as plain text.";

    private final Function<String, InferenceProvider> providerLookup;

    /**
     * Creates a transcribe function backed by the given provider lookup.
     *
     * @param providerLookup resolves model names to inference providers
     */
    public AiTranscribeFunction(Function<String, InferenceProvider> providerLookup) {
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
                    "ai_transcribe requires 2 arguments: model, file");
        }
        String modelName = extractString(arguments.get(0), "model");
        if (modelName == null) {
            return new ColumnValue.NullValue();
        }

        ColumnValue fileArg = arguments.get(1);
        if (fileArg instanceof ColumnValue.NullValue) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }

        // Verify audio modality support
        if (!provider.supportedModalities().contains(Modality.AUDIO)) {
            throw new IllegalStateException(
                    "Model '" + modelName + "' does not support audio transcription. "
                            + "Supported modalities: " + provider.supportedModalities());
        }

        MultimodalPrompt prompt = FileStructHelper.buildMultimodalPrompt(DEFAULT_PROMPT, fileArg);
        InferenceResult result = provider.completeMultimodal(prompt, GenerationParameters.DEFAULTS);
        return new ColumnValue.StringValue(result.text());
    }

    private static String extractString(ColumnValue cv, String argName) {
        if (cv instanceof ColumnValue.StringValue sv) {
            return sv.value();
        }
        if (cv instanceof ColumnValue.NullValue) {
            return null;
        }
        throw new IllegalArgumentException(
                "ai_transcribe: " + argName + " must be VARCHAR");
    }
}
