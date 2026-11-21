package org.pangrova.streaming.worker;

import org.pangrova.protocol.v2.TaskRequest;

/** Resolves the real source, decoder and staged writer for one admitted CDC task. */
@FunctionalInterface
public interface CdcWorkerTaskResourcesProvider {

    CdcWorkerTaskResources open(TaskRequest request);
}
