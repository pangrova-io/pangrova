package org.pangrova.connector.iceberg;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.concurrent.atomic.AtomicBoolean;

/** Deterministic concurrent-client commit immediately before a real SDK metadata CAS. */
final class IcebergCommitInterleavingOperations implements TableOperations {
    private final TableOperations actual;
    private final Runnable concurrentCommit;
    private final Runnable afterCommit;
    private final AtomicBoolean firstAttempt = new AtomicBoolean(true);

    IcebergCommitInterleavingOperations(TableOperations actual, Runnable concurrentCommit, Runnable afterCommit) {
        this.actual = actual;
        this.concurrentCommit = concurrentCommit;
        this.afterCommit = afterCommit;
    }

    @Override public TableMetadata current() { return actual.current(); }
    @Override public TableMetadata refresh() { return actual.refresh(); }
    @Override public void commit(TableMetadata base, TableMetadata metadata) {
        if (firstAttempt.compareAndSet(true, false)) concurrentCommit.run();
        actual.commit(base, metadata);
        afterCommit.run();
    }
    @Override public FileIO io() { return actual.io(); }
    @Override public EncryptionManager encryption() { return actual.encryption(); }
    @Override public String metadataFileLocation(String fileName) { return actual.metadataFileLocation(fileName); }
    @Override public LocationProvider locationProvider() { return actual.locationProvider(); }
    @Override public long newSnapshotId() { return actual.newSnapshotId(); }
    @Override public boolean requireStrictCleanup() { return actual.requireStrictCleanup(); }
}
