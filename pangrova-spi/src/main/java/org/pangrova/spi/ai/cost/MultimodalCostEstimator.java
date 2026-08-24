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
package org.pangrova.spi.ai.cost;

import org.pangrova.spi.ai.inference.Modality;
import org.pangrova.spi.annotation.Experimental;

import java.util.Map;
import java.util.Set;

/**
 * SPI for estimating the cost of multimodal AI operations.
 *
 * <p>Multimodal inference (image/audio/document) has significantly different
 * cost profiles compared to text-only operations. Images are typically billed
 * at a higher token rate, and audio/document processing may involve additional
 * preprocessing costs.</p>
 *
 * <p>Implementations should provide model-specific cost estimation that accounts
 * for input modality, file size, resolution (for images), and duration (for audio).</p>
 *
 * @since 1.1
 */
@Experimental
public interface MultimodalCostEstimator {

    /**
     * Estimates the effective token cost for a multimodal inference call.
     *
     * <p>The returned value represents "equivalent text tokens" — a normalized
     * unit that allows unified budget tracking across modalities. For example,
     * a single 1024x1024 image might be estimated as 765 equivalent tokens
     * (following OpenAI's vision pricing model).</p>
     *
     * @param request the cost estimation request containing modality and metadata
     * @return estimated cost in equivalent text tokens
     */
    long estimateTokenCost(CostEstimationRequest request);

    /**
     * Returns the cost multiplier for a given modality relative to text.
     *
     * <p>For example, IMAGE modality might return 4.0 (meaning image tokens
     * cost 4x text tokens), while AUDIO might return 2.5.</p>
     *
     * @param modality the input modality
     * @param modelName the model being used
     * @return multiplier relative to text token cost (1.0 for TEXT)
     */
    double getCostMultiplier(Modality modality, String modelName);

    /**
     * Returns the set of modalities supported by this estimator.
     */
    default Set<Modality> supportedModalities() {
        return Set.of(Modality.TEXT, Modality.IMAGE, Modality.AUDIO, Modality.DOCUMENT);
    }

    /**
     * Request object for cost estimation containing all relevant metadata.
     *
     * @param modelName    the AI model name
     * @param modality     the primary input modality
     * @param fileSizeBytes size of the input file in bytes (0 for text-only)
     * @param mimeType     MIME type of the input (null for text-only)
     * @param metadata     additional metadata (e.g., image resolution, audio duration)
     */
    record CostEstimationRequest(
            String modelName,
            Modality modality,
            long fileSizeBytes,
            String mimeType,
            Map<String, String> metadata) {

        public CostEstimationRequest(String modelName, Modality modality,
                                     long fileSizeBytes, String mimeType) {
            this(modelName, modality, fileSizeBytes, mimeType, Map.of());
        }

        /** Convenience factory for text-only requests. */
        public static CostEstimationRequest textOnly(String modelName, long textTokens) {
            return new CostEstimationRequest(modelName, Modality.TEXT, 0, null,
                    Map.of("text_tokens", String.valueOf(textTokens)));
        }

        /** Convenience factory for image requests. */
        public static CostEstimationRequest forImage(String modelName, long fileSizeBytes,
                                                     String mimeType, int width, int height) {
            return new CostEstimationRequest(modelName, Modality.IMAGE, fileSizeBytes, mimeType,
                    Map.of("width", String.valueOf(width), "height", String.valueOf(height)));
        }

        /** Convenience factory for audio requests. */
        public static CostEstimationRequest forAudio(String modelName, long fileSizeBytes,
                                                     String mimeType, double durationSeconds) {
            return new CostEstimationRequest(modelName, Modality.AUDIO, fileSizeBytes, mimeType,
                    Map.of("duration_seconds", String.valueOf(durationSeconds)));
        }

        /** Convenience factory for document requests. */
        public static CostEstimationRequest forDocument(String modelName, long fileSizeBytes,
                                                        String mimeType, int pageCount) {
            return new CostEstimationRequest(modelName, Modality.DOCUMENT, fileSizeBytes, mimeType,
                    Map.of("page_count", String.valueOf(pageCount)));
        }
    }
}
