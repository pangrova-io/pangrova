package org.pangrova.connector.iceberg;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/** Real SDK metadata/manifest commits with a deterministic, decreasing snapshot ID supplier. */
final class DeterministicSnapshotTable {
    private DeterministicSnapshotTable() { }

    static Table descendingIds(Table table) {
        TableOperations delegate = ((HasTableOperations) table).operations();
        var next = new AtomicLong(900);
        TableOperations operations = new TableOperations() {
            @Override public TableMetadata current() { return delegate.current(); }
            @Override public TableMetadata refresh() { return delegate.refresh(); }
            @Override public void commit(TableMetadata base, TableMetadata metadata) { delegate.commit(base, metadata); }
            @Override public FileIO io() { return delegate.io(); }
            @Override public EncryptionManager encryption() { return delegate.encryption(); }
            @Override public String metadataFileLocation(String fileName) { return delegate.metadataFileLocation(fileName); }
            @Override public LocationProvider locationProvider() { return delegate.locationProvider(); }
            @Override public long newSnapshotId() { return next.getAndAdd(-100); }
        };
        return new BaseTable(operations, table.name());
    }
}
