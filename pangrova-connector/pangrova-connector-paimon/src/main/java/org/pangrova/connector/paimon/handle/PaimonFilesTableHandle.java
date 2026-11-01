package org.pangrova.connector.paimon.handle;

import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.protocol.v2.PaimonFilesReadContext;
import org.pangrova.protocol.v2.PinnedMetadataObject;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;

import java.util.List;
import java.util.Objects;

/** Native file metadata relation with immutable SDK-planned source selections. */
public record PaimonFilesTableHandle(PaimonTableHandle baseTable, List<Manifest> manifests,
        String sourceIdentity, String tableUuid, long snapshotId,
        PinnedMetadataObject snapshot) implements ConnectorTableHandle {
    /** One exact manifest and the active physical file identities selected from it. */
    public record Manifest(String uri, long length, String sha256, long snapshotId,
            PaimonFilesReadContext context) {
        public Manifest {
            context = Objects.requireNonNull(context, "context").toBuilder().build();
            if (uri == null || uri.isBlank() || length <= 0 || snapshotId <= 0
                    || sha256 == null || !sha256.matches("[0-9a-f]{64}") || context.getFilesCount() == 0) {
                throw new IllegalArgumentException("Paimon file manifest requires exact identity and selected files");
            }
        }

        @Override
        public PaimonFilesReadContext context() {
            return context.toBuilder().build();
        }
    }

    public PaimonFilesTableHandle {
        Objects.requireNonNull(baseTable, "baseTable");
        manifests = List.copyOf(manifests);
        Objects.requireNonNull(sourceIdentity, "sourceIdentity");
        Objects.requireNonNull(tableUuid, "tableUuid");
        snapshot = Objects.requireNonNull(snapshot, "snapshot").toBuilder().build();
        if (snapshotId < 0 || (!sourceIdentity.isBlank() && tableUuid.isBlank())) {
            throw new IllegalArgumentException("Bound files relation requires its table incarnation");
        }
        if (sourceIdentity.isBlank() && !manifests.isEmpty()) {
            throw new IllegalArgumentException("An unbound files relation cannot carry sources");
        }
    }

    @Override
    public PinnedMetadataObject snapshot() {
        return snapshot.toBuilder().build();
    }

    public static PaimonFilesTableHandle selector(PaimonTableHandle base) {
        return new PaimonFilesTableHandle(base, List.of(), "", "", 0, PinnedMetadataObject.getDefaultInstance());
    }

    public boolean bound() { return !sourceIdentity.isBlank(); }
    public String tableName() { return baseTable.tableName() + "$files"; }

    public List<ConnectorTableMetadata.Column> typedColumns() {
        return PaimonTypeMapper.toPangrovaColumns(org.apache.paimon.table.system.FilesTable.TABLE_TYPE);
    }

    @Override
    public PaimonFilesTableHandle withTimeTravelSnapshot(long selectedSnapshotId) {
        return selector(baseTable.withTimeTravelSnapshot(selectedSnapshotId));
    }
}
