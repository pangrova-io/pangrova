package org.pangrova.connector.iceberg;

import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.pangrova.connector.iceberg.handle.IcebergDeclarationProperties;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;

import java.util.Objects;

/** Checks every SDK base used by retries, then delegates its atomic complete-metadata CAS. */
final class IcebergDeclarationTableOperations implements TableOperations {
    private final TableOperations delegate;
    private final IcebergOutputTableHandle handle;
    private boolean committed;

    IcebergDeclarationTableOperations(TableOperations delegate, IcebergOutputTableHandle handle) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.handle = Objects.requireNonNull(handle, "handle");
    }

    @Override
    public TableMetadata current() { return checked(delegate.current()); }

    @Override
    public TableMetadata refresh() { return checked(delegate.refresh()); }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        if (committed) {
            throw new IllegalStateException("Iceberg declaration-fenced operations already published a commit");
        }
        checked(base);
        checked(metadata);
        // A concurrent ALTER after checked(base) makes the backend CAS fail.
        // The SDK may retry a fresh data-only base, but checked(refresh())
        // rejects a changed declaration before any subsequent commit attempt.
        delegate.commit(base, metadata);
        committed = true;
    }

    private TableMetadata checked(TableMetadata metadata) {
        // Post-CAS refresh belongs to SDK cleanup, not admission. A subsequent
        // ALTER cannot undo an acknowledged publication; another commit through
        // these operations is forbidden even if the declaration is unchanged.
        if (committed || matches(metadata)) return metadata;
        throw new IllegalStateException("Iceberg write declaration changed since allocation: "
                + handle.schemaName() + "." + handle.tableName());
    }

    private boolean matches(TableMetadata metadata) {
        if (metadata == null) return false;
        return identityMatches(metadata) && handle.schemaJson().equals(SchemaParser.toJson(metadata.schema()))
                && handle.partitionSpecJson().equals(PartitionSpecParser.toJson(metadata.spec()))
                && handle.declarationProperties().equals(IcebergDeclarationProperties.capture(metadata.properties()));
    }

    private boolean identityMatches(TableMetadata metadata) {
        return handle.tableUuid().equals(metadata.uuid()) && handle.tableLocation().equals(metadata.location())
                && handle.formatVersion() == metadata.formatVersion();
    }

    @Override
    public FileIO io() { return delegate.io(); }

    @Override
    public EncryptionManager encryption() { return delegate.encryption(); }

    @Override
    public String metadataFileLocation(String fileName) { return delegate.metadataFileLocation(fileName); }

    @Override
    public LocationProvider locationProvider() { return delegate.locationProvider(); }

    @Override
    public long newSnapshotId() { return delegate.newSnapshotId(); }

    @Override
    public boolean requireStrictCleanup() { return delegate.requireStrictCleanup(); }
}
