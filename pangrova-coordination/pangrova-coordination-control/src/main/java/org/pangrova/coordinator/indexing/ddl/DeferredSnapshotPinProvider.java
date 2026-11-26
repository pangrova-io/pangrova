package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;

import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.persistence.generation.api.job.SnapshotPinIntent;
import org.pangrova.persistence.generation.api.enums.SnapshotPinState;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinReference;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.build.ScalarSnapshotPinLifecycle;
import org.pangrova.persistence.indexing.api.metadata.ScalarSnapshotPins.ScalarPin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.function.BooleanSupplier;
import org.pangrova.security.api.exception.AccessDeniedException;

import java.util.List;
import java.util.Objects;

/** Process lifecycle adapter with current-grant build access and persisted-scope DROPPED cleanup. */
public final class DeferredSnapshotPinProvider implements ScalarSnapshotPinLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(DeferredSnapshotPinProvider.class);
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
            var descriptor = requireDescriptor(indexId);
            var provider = requireProvider(catalog, schema, table, descriptor);
            if (!IndexDdlExecutor.requiresNativeArtifact(descriptor.indexType())
                    && !indexes.reserveScalarSnapshotPin(descriptor, snapshot, owner, Instant.now())) {
                throw new IllegalStateException("Scalar snapshot pin attempt is no longer reserved: " + owner.id());
            }
            provider.pinSnapshot(catalog, schema, table, snapshot, owner);
        });
    }

    @Override
    public void unpinSnapshot(String catalog, String schema, String table, String snapshot,
                              SnapshotPinOwner owner) {
        var descriptor = requireDescriptor(owner.indexId());
        var provider = requireProvider(catalog, schema, table, descriptor);
        if (builds != null && descriptor.indexType().requiresNativeArtifact()) {
            var intent = builds.findSnapshotPin(descriptor.tenantId(), owner.id());
            if (intent.isPresent()) {
                if (!intent.get().sourceSnapshot().equals(snapshot)) {
                    throw new IllegalStateException("Native snapshot pin differs from its durable source snapshot");
                }
                releaseDurablePin(descriptor, intent.get(), provider, catalog, schema, table, snapshot, owner);
                return;
            }
        }
        if (!IndexDdlExecutor.requiresNativeArtifact(descriptor.indexType())) {
            var scalar = indexes.findScalarSnapshotPin(owner);
            if (scalar.isPresent()) {
                ScalarPin pin = scalar.orElseThrow();
                requireExactScalarScope(pin, descriptor, snapshot);
                if ("RELEASED".equals(pin.state())) {
                    return;
                }
                ScalarPin claimed = indexes.claimScalarSnapshotPinCleanup(owner, Instant.now()).orElseThrow(
                        () -> new IllegalStateException("Scalar snapshot pin lacks durable drain/terminal proof"));
                releaseClaimedScalarPin(claimed, provider);
                return;
            }
        }
        provider.unpinSnapshot(catalog, schema, table, snapshot, owner);
    }

    @Override
    public void markBuildDrained(SnapshotPinOwner owner, String snapshot) {
        indexes.markScalarSnapshotPinDrained(owner, snapshot, Instant.now());
    }

    /** Releases only exact, positively drained scalar attempts after a durable terminal generation. */
    public int cleanupDrainedScalarPins(BooleanSupplier stillLeader) {
        int released = 0;
        for (ScalarPin candidate : indexes.listScalarSnapshotPinCleanupCandidates(Instant.now(), 100)) {
            if (!stillLeader.getAsBoolean()) {
                break;
            }
            try {
                boolean[] completed = {false};
                indexes.withIndexLifecycleLock(candidate.owner().indexId(), () -> {
                    if (!stillLeader.getAsBoolean()) {
                        return;
                    }
                    var pin = indexes.claimScalarSnapshotPinCleanup(candidate.owner(), Instant.now());
                    if (pin.isPresent()) {
                        ScalarPin claimed = pin.orElseThrow();
                        var descriptor = requireDescriptor(claimed.owner().indexId());
                        requireExactScalarScope(claimed, descriptor, claimed.snapshot());
                        var provider = requireProvider(
                                claimed.catalog(), claimed.schema(), claimed.table(), descriptor);
                        releaseClaimedScalarPin(claimed, provider);
                        completed[0] = true;
                    }
                });
                if (completed[0]) {
                    released++;
                }
            } catch (RuntimeException failure) {
                LOG.warn("event=scalar_snapshot_pin_cleanup_retry owner={} error={}",
                        candidate.owner().id(), failure.getMessage(), failure);
            }
        }
        return released;
    }

    private void releaseClaimedScalarPin(ScalarPin pin, SnapshotPinProvider provider) {
        provider.unpinSnapshot(pin.catalog(), pin.schema(), pin.table(), pin.snapshot(), pin.owner());
        indexes.markScalarSnapshotPinReleased(pin.owner(), pin.snapshot(), Instant.now());
        LOG.info("event=scalar_snapshot_pin_released owner={} snapshot={}", pin.owner().id(), pin.snapshot());
    }

    private static void requireExactScalarScope(ScalarPin pin,
            org.pangrova.indexing.api.descriptor.IndexDescriptor descriptor, String snapshot) {
        if (!pin.tenantId().equals(descriptor.tenantId()) || !pin.catalog().equals(descriptor.catalogName())
                || !pin.schema().equals(descriptor.schemaName()) || !pin.table().equals(descriptor.tableName())
                || !pin.snapshot().equals(snapshot)
                || IndexDdlExecutor.requiresNativeArtifact(descriptor.indexType())) {
            throw new AccessDeniedException("Scalar snapshot cleanup differs from the exact original source");
        }
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
        if (builds == null && descriptor.indexType().requiresNativeArtifact()) {
            throw new AccessDeniedException("Snapshot maintenance requires a persisted current authorization scope");
        }
        if (!descriptor.catalogName().equals(catalog) || !descriptor.schemaName().equals(schema)
                || !descriptor.tableName().equals(table)) {
            throw new AccessDeniedException("Snapshot maintenance differs from its exact index scope");
        }
        var authority = descriptor.indexType().requiresNativeArtifact()
                ? DeferredIndexAuthorityResolver.resolveExisting(builds, indexes, descriptor)
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
