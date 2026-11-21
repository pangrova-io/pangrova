package org.pangrova.streaming.exception;

import org.pangrova.common.api.annotation.Experimental;

/** Raised when the owning Worker task is cancelled during bounded CDC ingestion. */
@Experimental
public final class CdcIngestCancelledException extends RuntimeException {

    public CdcIngestCancelledException(String streamId) {
        super("CDC range ingest was cancelled for stream " + streamId);
    }
}
