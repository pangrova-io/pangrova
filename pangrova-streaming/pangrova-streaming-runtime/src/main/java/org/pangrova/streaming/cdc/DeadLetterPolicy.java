package org.pangrova.streaming.cdc;

/**
 * How a CDC pipeline reacts to records that the source format cannot convert.
 *
 * <p>{@link #FAIL_PIPELINE} preserves the existing data-safety semantics: a
 * malformed record fails the batch and is retried until the configured poison
 * threshold stops the pipeline. {@link #SKIP_AND_RECORD} is opt-in and requires
 * a {@link CdcDeadLetterHandler} that is {@link CdcDeadLetterHandler#isDurable()
 * durable} so the skipped payload is not silently lost; the pipeline refuses to
 * start otherwise.</p>
 */
public enum DeadLetterPolicy {

    /** Fail the batch; after the retry threshold the pipeline stops. */
    FAIL_PIPELINE,

    /**
     * Record the payload through a durable dead-letter handler and continue
     * the batch.
     */
    SKIP_AND_RECORD
}
