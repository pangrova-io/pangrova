package org.pangrova.ai.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Input modalities shared by inference, embedding, reranking, and governance. */
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
