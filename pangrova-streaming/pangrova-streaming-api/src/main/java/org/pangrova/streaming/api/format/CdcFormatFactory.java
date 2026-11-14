package org.pangrova.streaming.api.format;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;

/** Creates one stateless, thread-safe decoder shared by the pipelines of a Coordinator.
 * Factory discovery and decoder construction must not acquire external resources. */
@Experimental
public interface CdcFormatFactory {
    /** Stable nonblank, case-sensitive SQL FORMAT value, unique within the Coordinator. */
    String identifier();

    /** Returns a decoder whose formatId equals identifier(), once per Coordinator registry. */
    CdcRecordConverter create();
}
