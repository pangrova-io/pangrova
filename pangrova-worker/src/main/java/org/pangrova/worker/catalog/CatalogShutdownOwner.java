package org.pangrova.worker.catalog;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.connector.api.plugin.Connector;

/** Retains an initializing or failed connector until its actual shutdown returns. */
@ThreadSafe
final class CatalogShutdownOwner {
    private Connector connector;
    private boolean initializing = true;
    private boolean shuttingDown;
    private boolean terminated;

    synchronized void attach(Connector value) {
        if (connector != null || !initializing) {
            throw new IllegalStateException("Catalog connector ownership already assigned");
        }
        connector = java.util.Objects.requireNonNull(value, "connector");
    }

    synchronized void initializationComplete() {
        initializing = false;
    }

    void shutdown() {
        Connector owned;
        synchronized (this) {
            if (terminated) return;
            if (initializing || shuttingDown) {
                throw new IllegalStateException("Catalog initialization or shutdown is still in progress");
            }
            shuttingDown = true;
            owned = connector;
        }
        boolean completed = false;
        try {
            if (owned != null) owned.shutdown();
            completed = true;
        } finally {
            synchronized (this) {
                if (completed) {
                    connector = null;
                    terminated = true;
                }
                shuttingDown = false;
            }
        }
    }
}
