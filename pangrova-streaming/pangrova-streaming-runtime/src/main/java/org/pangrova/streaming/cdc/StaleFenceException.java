package org.pangrova.streaming.cdc;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a CDC pipeline attempts to write or commit but its fencing epoch
 * is stale — meaning a new leader has taken over and the current pipeline
 * instance is a zombie.
 *
 * <p>This exception is non-retryable: the pipeline must stop immediately to
 * prevent data corruption or duplicates from a deposed leader.</p>
 *
 * <p>Fencing scenario:</p>
 * <ol>
 *   <li>Pipeline A acquires leadership at epoch N and begins processing.</li>
 *   <li>Pipeline A's lease expires (GC pause, network partition).</li>
 *   <li>Pipeline B acquires leadership at epoch N+1.</li>
 *   <li>Pipeline A resumes — before writing, it checks its epoch (N) against
 *       the current epoch (N+1). The check fails, and this exception is thrown.</li>
 *   <li>Pipeline A halts, preventing zombie writes.</li>
 * </ol>
 *
 * @see CdcFencingToken
 */
public final class StaleFenceException extends PangrovaException {

    private final String streamName;
    private final long pipelineEpoch;

    /** Creates an exception for the given stream whose pipeline epoch has been superseded. */
    public StaleFenceException(String streamName, long pipelineEpoch) {
        super(PangrovaErrorCode.STREAMING_CDC_STALE_FENCE,
                "CDC pipeline fencing token is stale: stream=" + streamName
                        + " pipelineEpoch=" + pipelineEpoch
                        + " — a new leader has taken over; halting zombie pipeline");
        this.streamName = streamName;
        this.pipelineEpoch = pipelineEpoch;
    }

    /** The CDC stream name of the fenced pipeline. */
    public String streamName() {
        return streamName;
    }

    /** The epoch this pipeline was assigned at start time. */
    public long pipelineEpoch() {
        return pipelineEpoch;
    }
}
