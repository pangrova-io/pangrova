package org.pangrova.ai.api.enums;

/** Reason an inference invocation stopped producing output. */
public enum AiFinishReason {
    COMPLETE,
    LENGTH_LIMIT,
    CONTENT_FILTERED
}
