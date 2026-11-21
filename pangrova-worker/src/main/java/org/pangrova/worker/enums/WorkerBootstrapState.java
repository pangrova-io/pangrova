package org.pangrova.worker.enums;

import org.pangrova.common.api.annotation.Internal;

/** Admission and cleanup phases of a single Worker process. */
@Internal
public enum WorkerBootstrapState {
    NEW,
    CONFIGURING,
    STARTING,
    RUNNING,
    STOPPING,
    CLEANUP_PENDING,
    STOPPED
}
