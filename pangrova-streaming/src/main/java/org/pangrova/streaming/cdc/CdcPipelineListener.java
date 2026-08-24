package org.pangrova.streaming.cdc;

/**
 * Callback interface for CDC pipeline lifecycle events.
 *
 * <p>Implementations are invoked by {@link CdcPipelineLoop} when the pipeline
 * transitions to a terminal failure state (consecutive failures exceed threshold).
 * The coordinator-side scheduler uses this to update stream lifecycle state.</p>
 */
public interface CdcPipelineListener {

    /**
     * Called when the pipeline fails after exceeding the consecutive failure threshold.
     *
     * @param cause the exception that triggered the failure transition
     */
    void onFailed(Exception cause);
}
