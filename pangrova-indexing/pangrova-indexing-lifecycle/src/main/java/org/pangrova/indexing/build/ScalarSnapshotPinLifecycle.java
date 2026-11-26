package org.pangrova.indexing.build;

import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinProvider;

/** Production scalar pin adapter that persists completion before asynchronous release. */
public interface ScalarSnapshotPinLifecycle extends SnapshotPinProvider {
    /** Called by the exact attempt after its synchronous source-reading stack exits. */
    void markBuildDrained(SnapshotPinOwner owner, String snapshot);
}
