package org.pangrova.coordination.zookeeper;

import org.pangrova.persistence.coordination.api.exception.CoordinationStorageException;

/** Preserves interruption and the original failure at the quorum boundary. */
final class ZooKeeperFailures {
    private ZooKeeperFailures() {}

    static CoordinationStorageException storageFailure(String operation, String key, Exception cause) {
        if (cause instanceof InterruptedException) Thread.currentThread().interrupt();
        if (cause instanceof CoordinationStorageException failure) return failure;
        return new CoordinationStorageException("ZooKeeper coordination " + operation + " failed: " + key, cause);
    }
}
