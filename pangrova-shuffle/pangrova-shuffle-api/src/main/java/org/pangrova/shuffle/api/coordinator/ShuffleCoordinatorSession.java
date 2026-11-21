package org.pangrova.shuffle.api.coordinator;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;

/** Owns one external provider's allocations, task grants and control connections in this process. */
@Experimental(reason = "Fenced, externally published exchanges")
public interface ShuffleCoordinatorSession extends ShuffleLifecycleManager, ShuffleTaskAccess, AutoCloseable {
    /** Stops admission, drains outstanding operations and closes the owned backend resources. */
    @Override
    void close();
}
