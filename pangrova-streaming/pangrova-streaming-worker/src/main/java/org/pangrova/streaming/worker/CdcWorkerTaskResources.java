package org.pangrova.streaming.worker;

import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.cdc.CdcRangeBatchWriter;

/**
 * Exact task-scoped CDC resources resolved by the Worker composition root.
 *
 * <p>The provider must not publish the target catalog; the writer only stages
 * fragments for the Coordinator's journaled commit.</p>
 */
public interface CdcWorkerTaskResources extends AutoCloseable {

    CdcSourceReader sourceReader();

    CdcRecordConverter converter();

    CdcRangeBatchWriter writer();

    @Override
    void close();
}
