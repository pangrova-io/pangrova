package org.pangrova.coordinator.adapter.indexing.ddl;

import org.pangrova.generation.api.job.GenerationJobStore;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.connector.spi.index.SnapshotPinProvider;
import org.pangrova.indexing.api.state.IndexState;
import org.pangrova.security.api.AccessDeniedException;

import java.util.List;
import java.util.Objects;

/** Process lifecycle adapter with current-grant build access and persisted-scope DROPPED cleanup. */
public final class DeferredSnapshotPinProvider implements SnapshotPinProvider {
    private final IndexMetadataStore indexes;
    private final GenerationJobStore builds;
    private final ConnectorAccessorResolver accessors;

    /** Creates a lazy adapter without opening connector metadata under a synthetic startup subject. */
    public DeferredSnapshotPinProvider(IndexMetadataStore indexes, GenerationJobStore builds,
                                        ConnectorAccessorResolver accessors) {
        this.indexes = Objects.requireNonNull(indexes, "indexes");
        this.builds = builds;
        this.accessors = Objects.requireNonNull(accessors, "accessors");
    }

    @Override
    public void pinSnapshot(String catalog, String schema, String table, long snapshot, String indexId) {
        indexes.withIndexLifecycleLock(indexId, () -> {
            if (!indexes.isIndexSnapshotBuildable(indexId, Long.toString(snapshot))) {
                throw new AccessDeniedException("Snapshot is not reserved by an exact building index generation");
            }
            var provider = requireProvider(catalog, schema, table, indexId);
            provider.pinSnapshot(catalog, schema, table, snapshot, indexId);
        });
    }

    @Override
    public void unpinSnapshot(String catalog, String schema, String table, long snapshot, String indexId) {
        var provider = requireProvider(catalog, schema, table, indexId);
        provider.unpinSnapshot(catalog, schema, table, snapshot, indexId);
    }

    @Override
    public List<Long> getPinnedSnapshotsForIndex(String catalog, String schema, String table, String indexId) {
        return requireProvider(catalog, schema, table, indexId)
                .getPinnedSnapshotsForIndex(catalog, schema, table, indexId);
    }

    @Override
    public List<Long> getPinnedSnapshots(String catalog, String schema, String table) {
        throw new AccessDeniedException("Deferred snapshot enumeration requires an exact authorized index subject");
    }

    private SnapshotPinProvider requireProvider(
            String catalog, String schema, String table, String indexId) {
        var descriptor = indexes.findIndex(indexId).orElseThrow(() ->
                new AccessDeniedException("Snapshot maintenance index no longer exists"));
        if (builds == null && IndexDdlExecutor.requiresNativeArtifact(descriptor.indexType())) {
            throw new AccessDeniedException("Snapshot maintenance requires a persisted current authorization scope");
        }
        if (!descriptor.catalogName().equals(catalog) || !descriptor.schemaName().equals(schema)
                || !descriptor.tableName().equals(table)) {
            throw new AccessDeniedException("Snapshot maintenance differs from its exact index scope");
        }
        var authority = IndexDdlExecutor.requiresNativeArtifact(descriptor.indexType())
                ? DeferredIndexAuthorityResolver.resolveExisting(builds, descriptor)
                : DeferredIndexAuthorityResolver.resolveScalar(
                        descriptor, descriptor.state() == IndexState.DROPPED
                                ? indexes.loadDroppedIndexCleanupPrincipal(descriptor)
                                : indexes.loadAuthorizedBuildPrincipal(descriptor));
        var provider = accessors.forSession(authority.session()).resolveSnapshotPinProvider();
        if (provider == null) {
            throw new IllegalStateException("Authorized catalog does not support snapshot pinning");
        }
        return provider;
    }
}
