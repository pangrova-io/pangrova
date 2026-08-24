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

/**
 * Default implementation of {@link MultimodalCostEstimator} using industry-standard
 * pricing models for multimodal token estimation.
 *
 * <p>Cost model follows OpenAI/Anthropic conventions:</p>
 * <ul>
 *   <li>TEXT: 1.0x (baseline)</li>
 *   <li>IMAGE: 4.0x — high-res images consume ~765 tokens per 512x512 tile</li>
 *   <li>AUDIO: 2.5x — Whisper-style per-second billing</li>
 *   <li>DOCUMENT: 3.0x — PDF page-based estimation</li>
 *   <li>VIDEO: 6.0x — frame sampling + audio track</li>
 * </ul>
 *
 * @since 1.1
 */
@Experimental
public final class DefaultMultimodalCostEstimator implements MultimodalCostEstimator {

    /** Tokens per 512x512 image tile (OpenAI vision pricing). */
    private static final long TOKENS_PER_IMAGE_TILE = 765;

    /** Base tokens for an image (fixed overhead). */
    private static final long IMAGE_BASE_TOKENS = 85;

    /** Tile size in pixels. */
    private static final int TILE_SIZE = 512;

    /** Tokens per second of audio (Whisper pricing model). */
    private static final double TOKENS_PER_AUDIO_SECOND = 25.0;

    /** Estimated tokens per PDF page. */
    private static final long TOKENS_PER_PAGE = 800;

    /** Fallback estimate when file size is the only available metadata. */
    private static final double TOKENS_PER_KB = 1.5;

    private static final int IMAGE_FALLBACK_KB_PER_TILE = 25;
    private static final int DOCUMENT_FALLBACK_KB_PER_PAGE = 50;
    private static final double VIDEO_FALLBACK_KB_PER_SECOND = 625.0;

    // Cost multipliers by modality
    private static final double TEXT_MULTIPLIER = 1.0;
    private static final double IMAGE_MULTIPLIER = 4.0;
    private static final double AUDIO_MULTIPLIER = 2.5;
    private static final double DOCUMENT_MULTIPLIER = 3.0;
    private static final double VIDEO_MULTIPLIER = 6.0;

    @Override
    public long estimateTokenCost(CostEstimationRequest request) {
        if (request == null) {
            return 0;
        }
        return switch (request.modality()) {
            case TEXT -> estimateTextCost(request);
            case IMAGE -> estimateImageCost(request);
            case AUDIO -> estimateAudioCost(request);
            case DOCUMENT -> estimateDocumentCost(request);
            case VIDEO -> estimateVideoCost(request);
        };
    }

    @Override
    public double getCostMultiplier(Modality modality, String modelName) {
        if (modality == null) {
            return TEXT_MULTIPLIER;
        }
        return switch (modality) {
            case TEXT -> TEXT_MULTIPLIER;
            case IMAGE -> IMAGE_MULTIPLIER;
            case AUDIO -> AUDIO_MULTIPLIER;
            case DOCUMENT -> DOCUMENT_MULTIPLIER;
            case VIDEO -> VIDEO_MULTIPLIER;
        };
    }

    private long estimateTextCost(CostEstimationRequest request) {
        String textTokens = request.metadata().get("text_tokens");
        if (textTokens != null) {
            try {
                return Long.parseLong(textTokens);
            } catch (NumberFormatException e) {
                // Malformed metadata, fall through to size-based estimate
            }
        }
        // Rough estimate: 1 token ≈ 4 bytes for English text
        return Math.max(1, request.fileSizeBytes() / 4);
    }

    private long estimateImageCost(CostEstimationRequest request) {
        String widthStr = request.metadata().get("width");
        String heightStr = request.metadata().get("height");

        if (widthStr != null && heightStr != null) {
            try {
                int width = Integer.parseInt(widthStr);
                int height = Integer.parseInt(heightStr);
                // Calculate number of tiles needed
                int tilesX = (int) Math.ceil((double) width / TILE_SIZE);
                int tilesY = (int) Math.ceil((double) height / TILE_SIZE);
                int totalTiles = tilesX * tilesY;
                return (long) ((IMAGE_BASE_TOKENS + totalTiles * TOKENS_PER_IMAGE_TILE) * IMAGE_MULTIPLIER);
            } catch (NumberFormatException e) {
                // Malformed metadata, fall through to size-based estimate
            }
        }

        // Fallback: estimate from file size
        // Average JPEG compression ratio means 100KB ≈ 1024x768 ≈ 4 tiles
        long estimatedTiles = Math.max(
                1,
                request.fileSizeBytes() / (IMAGE_FALLBACK_KB_PER_TILE * 1024));
        return (long) ((IMAGE_BASE_TOKENS + estimatedTiles * TOKENS_PER_IMAGE_TILE) * IMAGE_MULTIPLIER);
    }

    private long estimateAudioCost(CostEstimationRequest request) {
        String durationStr = request.metadata().get("duration_seconds");
        if (durationStr != null) {
            try {
                double durationSeconds = Double.parseDouble(durationStr);
                return (long) (durationSeconds * TOKENS_PER_AUDIO_SECOND * AUDIO_MULTIPLIER);
            } catch (NumberFormatException e) {
                // Malformed metadata, fall through to size-based estimate
            }
        }
        // Fallback: estimate from file size (128kbps MP3 ≈ 16KB/sec)
        double estimatedSeconds = request.fileSizeBytes() / (16.0 * 1024);
        return (long) Math.max(1, estimatedSeconds * TOKENS_PER_AUDIO_SECOND * AUDIO_MULTIPLIER);
    }

    private long estimateDocumentCost(CostEstimationRequest request) {
        String pageCountStr = request.metadata().get("page_count");
        if (pageCountStr != null) {
            try {
                int pageCount = Integer.parseInt(pageCountStr);
                return (long) (pageCount * TOKENS_PER_PAGE * DOCUMENT_MULTIPLIER);
            } catch (NumberFormatException e) {
                // Malformed metadata, fall through to size-based estimate
            }
        }
        // Fallback: estimate 1 page per 50KB for PDF
        long estimatedPages = Math.max(
                1,
                request.fileSizeBytes() / (DOCUMENT_FALLBACK_KB_PER_PAGE * 1024));
        return (long) (estimatedPages * TOKENS_PER_PAGE * DOCUMENT_MULTIPLIER);
    }

    private long estimateVideoCost(CostEstimationRequest request) {
        String durationStr = request.metadata().get("duration_seconds");
        if (durationStr != null) {
            try {
                double durationSeconds = Double.parseDouble(durationStr);
                // Video: 1 frame/sec sampled + audio track
                long frameTokens = (long) (durationSeconds * TOKENS_PER_IMAGE_TILE);
                long audioTokens = (long) (durationSeconds * TOKENS_PER_AUDIO_SECOND);
                return (long) ((frameTokens + audioTokens) * VIDEO_MULTIPLIER);
            } catch (NumberFormatException e) {
                // Malformed metadata, fall through to size-based estimate
            }
        }
        // Fallback from file size (5Mbps video ≈ 625KB/sec)
        double estimatedSeconds = request.fileSizeBytes() / (VIDEO_FALLBACK_KB_PER_SECOND * 1024);
        return (long) Math.max(1, estimatedSeconds * TOKENS_PER_KB * VIDEO_MULTIPLIER * 1024);
    }
}
