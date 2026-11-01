package org.pangrova.streaming.cdc;

/**
 * Callback invoked for a payload that cannot be converted when the pipeline
 * uses {@link DeadLetterPolicy#SKIP_AND_RECORD}.
 *
 * <p>A handler that only logs does NOT satisfy this contract: the payload must
 * be durably persisted or published. Implementations declare persistence with
 * {@link #isDurable()}; the pipeline refuses to start with
 * {@code SKIP_AND_RECORD} unless the handler is durable. Handler failures fail
 * the batch.</p>
 */
@FunctionalInterface
public interface CdcDeadLetterHandler {

    void onDeadLetter(String streamName, byte[] payload, Exception cause);

    /**
     * Whether skipped payloads are durably retained (not just logged).
     *
     * @return true when a dead-letter record survives a process restart
     */
    default boolean isDurable() {
        return false;
    }
}
