package org.pangrova.coordinator.index.ddl;

import org.pangrova.metadata.artifact.build.ArtifactBuildStore;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.spi.connector.index.SnapshotPinProvider;
import org.pangrova.spi.security.AccessDeniedException;

import java.util.List;
import java.util.Objects;

/** Process lifecycle adapter; connector access is resolved only after current exact index authorization. */
public final class DeferredSnapshotPinProvider implements SnapshotPinProvider {
    private final IndexMetadataStore indexes;
    private final ArtifactBuildStore builds;
    private final ConnectorAccessorResolver accessors;

    /** Creates a lazy adapter without opening connector metadata under a synthetic startup subject. */
    public DeferredSnapshotPinProvider(IndexMetadataStore indexes, ArtifactBuildStore builds,
                                        ConnectorAccessorResolver accessors) {
        this.indexes = Objects.requireNonNull(indexes, "indexes");
        this.builds = builds;
        this.accessors = Objects.requireNonNull(accessors, "accessors");
    }

    @Override
    public void pinSnapshot(String catalog, String schema, String table, long snapshot, String indexId) {
        var provider = requireProvider(catalog, schema, table, snapshot, indexId);
        provider.pinSnapshot(catalog, schema, table, snapshot, indexId);
    }

    @Override
    public void unpinSnapshot(String catalog, String schema, String table, long snapshot, String indexId) {
        var provider = requireProvider(catalog, schema, table, snapshot, indexId);
        provider.unpinSnapshot(catalog, schema, table, snapshot, indexId);
    }

    @Override
    public List<Long> getPinnedSnapshots(String catalog, String schema, String table) {
        throw new AccessDeniedException("Deferred snapshot enumeration requires an exact authorized index subject");
    }

    private SnapshotPinProvider requireProvider(
            String catalog, String schema, String table, long snapshot, String indexId) {
        if (builds == null) {
            throw new AccessDeniedException("Snapshot maintenance requires a persisted current authorization scope");
        }
        var descriptor = indexes.findIndex(indexId).orElseThrow(() ->
                new AccessDeniedException("Snapshot maintenance index no longer exists"));
        if (!descriptor.catalogName().equals(catalog) || !descriptor.schemaName().equals(schema)
                || !descriptor.tableName().equals(table)
                || !Long.toString(snapshot).equals(descriptor.buildSnapshotId())) {
            throw new AccessDeniedException("Snapshot maintenance differs from its exact index scope");
        }
        var authority = DeferredIndexAuthorityResolver.resolveExisting(builds, descriptor);
        var provider = accessors.forSession(authority.session()).resolveSnapshotPinProvider();
        if (provider == null) {
            throw new IllegalStateException("Authorized catalog does not support snapshot pinning");
        }
        return provider;
    }
}
