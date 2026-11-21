package org.pangrova.ai.worker.invocation;

/** Encoded HTTP entity bytes and engine work counts, never LLM token usage. */
record AiResourceAmounts(long items, long attempts, long requestBytes, long responseBytes, long mediaBytes) {
    AiResourceAmounts {
        if (items < 0 || attempts < 0 || requestBytes < 0 || responseBytes < 0 || mediaBytes < 0) {
            throw new IllegalArgumentException("AI resources must be nonnegative");
        }
    }

    AiResourceAmounts minus(AiResourceAmounts value) {
        return new AiResourceAmounts(items - value.items, attempts - value.attempts,
                requestBytes - value.requestBytes, responseBytes - value.responseBytes,
                mediaBytes - value.mediaBytes);
    }

    AiResourceAmounts plus(AiResourceAmounts value) {
        return new AiResourceAmounts(Math.addExact(items, value.items), Math.addExact(attempts, value.attempts),
                Math.addExact(requestBytes, value.requestBytes), Math.addExact(responseBytes, value.responseBytes),
                Math.addExact(mediaBytes, value.mediaBytes));
    }
}
