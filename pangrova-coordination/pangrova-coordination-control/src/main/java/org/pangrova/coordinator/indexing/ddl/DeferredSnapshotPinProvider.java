package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;

import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.persistence.generation.api.job.SnapshotPinIntent;
import org.pangrova.persistence.generation.api.job.SnapshotPinState;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinReference;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.security.api.exception.AccessDeniedException;

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
    public void pinSnapshot(String catalog, String schema, String table, String snapshot,
                            SnapshotPinOwner owner) {
        String indexId = owner.indexId();
        indexes.withIndexLifecycleLock(indexId, () -> {
            var provider = requireProvider(catalog, schema, table, indexId);
            provider.pinSnapshot(catalog, schema, table, snapshot, owner);
        });
    }

    @Override
    public void unpinSnapshot(String catalog, String schema, String table, String snapshot,
                              SnapshotPinOwner owner) {
        var descriptor = requireDescriptor(owner.indexId());
        var provider = requireProvider(catalog, schema, table, descriptor);
        if (builds != null && IndexDdlExecutor.requiresNativeArtifact(descriptor.indexType())) {
            var intent = builds.findSnapshotPin(descriptor.tenantId(), owner.id());
            if (intent.isPresent()) {
                if (!intent.get().sourceSnapshot().equals(snapshot)) {
                    throw new IllegalStateException("Native snapshot pin differs from its durable source snapshot");
                }
                releaseDurablePin(descriptor, intent.get(), provider, catalog, schema, table, snapshot, owner);
                return;
            }
        }
        provider.unpinSnapshot(catalog, schema, table, snapshot, owner);
    }

    @Override
    public void validateSnapshotPin(String catalog, String schema, String table, String snapshot,
                                    SnapshotPinOwner owner) {
        var provider = requireProvider(catalog, schema, table, owner.indexId());
        provider.validateSnapshotPin(catalog, schema, table, snapshot, owner);
    }

    @Override
    public List<SnapshotPinReference> getSnapshotPinsForIndex(
            String catalog, String schema, String table, String indexId) {
        return requireProvider(catalog, schema, table, indexId)
                .getSnapshotPinsForIndex(catalog, schema, table, indexId);
    }

    @Override
    public List<String> getPinnedSnapshots(String catalog, String schema, String table) {
        throw new AccessDeniedException("Deferred snapshot enumeration requires an exact authorized index subject");
    }

    private SnapshotPinProvider requireProvider(
            String catalog, String schema, String table, String indexId) {
        return requireProvider(catalog, schema, table, requireDescriptor(indexId));
    }

    private SnapshotPinProvider requireProvider(
            String catalog, String schema, String table,
            org.pangrova.indexing.api.descriptor.IndexDescriptor descriptor) {
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

    private org.pangrova.indexing.api.descriptor.IndexDescriptor requireDescriptor(String indexId) {
        return indexes.findIndex(indexId).orElseThrow(() ->
                new AccessDeniedException("Snapshot maintenance index no longer exists"));
    }

    private void releaseDurablePin(
            org.pangrova.indexing.api.descriptor.IndexDescriptor descriptor,
            SnapshotPinIntent intent, SnapshotPinProvider provider,
            String catalog, String schema, String table, String snapshot, SnapshotPinOwner owner) {
        if (intent.state() == SnapshotPinState.RELEASED) {
            return;
        }
        if (!builds.beginSnapshotPinRelease(descriptor.tenantId(), intent.pinOwner(), java.time.Instant.now())) {
            throw new IllegalStateException("Native snapshot pin still has live execution references: " + owner.id());
        }
        provider.unpinSnapshot(catalog, schema, table, snapshot, owner);
        if (!builds.markSnapshotPinReleased(descriptor.tenantId(), intent.pinOwner(), java.time.Instant.now())) {
            throw new IllegalStateException("Released Native snapshot pin is absent from the durable ledger");
        }
    }
}
