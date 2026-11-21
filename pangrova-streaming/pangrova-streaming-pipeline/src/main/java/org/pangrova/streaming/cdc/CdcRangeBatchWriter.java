package org.pangrova.streaming.cdc;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.streaming.api.cdc.CdcEnvelope;

import java.util.List;

/** Writes decoded CDC envelopes without publishing the target catalog snapshot. */
@FunctionalInterface
@Experimental
public interface CdcRangeBatchWriter {

    /**
     * Stages one decoded batch and returns the exact commit fragments it produced.
     * The caller persists the fragments and publishes them through the distributed
     * write journal; this writer must not acknowledge source offsets.
     */
    List<byte[]> writeBatch(List<CdcEnvelope> envelopes);
}
