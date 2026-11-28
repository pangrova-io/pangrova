package org.pangrova.worker.catalog;

import org.pangrova.common.concurrency.ThreadSafe;
import java.util.concurrent.atomic.AtomicBoolean;

/** Idempotent exact-deployment lease held until the Native metadata operation returns. */
@ThreadSafe
public final class CatalogControlLease implements AutoCloseable {
    private final Runnable release;
    private final org.pangrova.protocol.v2.ObjectStoreExecutionBinding storage;
    private final AtomicBoolean released = new AtomicBoolean();

    CatalogControlLease(Runnable release, org.pangrova.protocol.v2.ObjectStoreExecutionBinding storage) {
        this.release = release;
        this.storage = storage;
    }

    public org.pangrova.protocol.v2.ObjectStoreExecutionBinding storageBinding() {
        if (released.get()) throw new IllegalStateException("Catalog control lease is closed");
        if (storage == null) throw new IllegalStateException("Catalog does not own Native dataset storage");
        return storage;
    }

    public void requireDatasetLocation(String location) {
        NativeObjectStoreBindingFactory.requireLocation(storageBinding(), location);
    }

    @Override
    public void close() {
        if (released.compareAndSet(false, true)) release.run();
    }
}
