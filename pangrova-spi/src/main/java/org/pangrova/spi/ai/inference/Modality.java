package org.pangrova.spi.ai.inference;

import org.pangrova.spi.annotation.Experimental;

/**
 * Modalities supported by multimodal inference providers.
 */
@Experimental
public enum Modality {
    /** Plain text input. */
    TEXT,
    /** Image input (JPEG, PNG, WebP, GIF). */
    IMAGE,
    /** Audio input (MP3, WAV, FLAC). */
    AUDIO,
    /** Video input (MP4, WebM). */
    VIDEO,
    /** Document input (PDF, DOCX, PPTX). */
    DOCUMENT
}
