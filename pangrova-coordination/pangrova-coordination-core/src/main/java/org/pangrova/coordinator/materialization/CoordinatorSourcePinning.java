package org.pangrova.coordinator.materialization;

import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.port.SourcePinning;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.connector.spi.materialization.ChangeProof;
import org.pangrova.connector.spi.materialization.ChangeSetMode;
import org.pangrova.connector.spi.materialization.ChangeSetPlan;
import org.pangrova.connector.spi.materialization.MaterializationCapabilities;
import org.pangrova.connector.spi.materialization.PinnedSource;
import org.pangrova.connector.spi.materialization.SourceProvider;
import org.pangrova.connector.spi.materialization.SourceRef;
import org.pangrova.connector.spi.materialization.SourceSnapshotRef;

/** Resolves the owning connector of each source and delegates exact snapshot holds. */
final class CoordinatorSourcePinning implements SourcePinning {

    private final CatalogRegistry catalogs;
    private final java.util.Set<String> validatedCapabilities =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    CoordinatorSourcePinning(CatalogRegistry catalogs) {
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
    }

    @Override
    public PinnedSource pin(String tenant, SourceRef source, SourceSnapshotRef expected,
            String pinOwner) {
        SourceProvider provider = provider(source);
        requireCompatibleCapabilities(provider, source);
        return provider.pin(source, tenant, expected, pinOwner);
    }

    @Override
    public ChangeSetPlan changeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode) {
        if (!from.source().equals(to.source())) {
            throw MaterializationException.requestRejected(
                    "a change set requires two pinned snapshots of the same source");
        }
        SourceProvider provider = provider(from.source());
        MaterializationCapabilities capabilities = capabilities(provider, from.source());
        if (!capabilities.supports(mode)) {
            throw MaterializationException.capabilityUnavailable(
                    "connector " + provider.connectorId() + " cannot serve " + mode
                            + " change sets for " + from.source().canonicalName());
        }
        return provider.changeSet(from, to, mode);
    }

    @Override
    public ChangeProof verify(ChangeSetPlan plan) {
        return provider(plan.source()).verify(plan);
    }

    @Override
    public void unpin(PinnedSource pinned) {
        provider(pinned.source()).unpin(pinned);
    }

    private SourceProvider provider(SourceRef source) {
        SourceProvider provider = catalogs.getCatalog(source.catalog())
                .getMaterializationSourceProvider()
                .orElseThrow(() -> MaterializationException.capabilityUnavailable(
                        "catalog " + source.catalog()
                                + " cannot pin materialization source snapshots"));
        if (!provider.connectorId().equals(source.connectorId())) {
            throw MaterializationException.requestRejected(
                    "source connector " + source.connectorId() + " does not match catalog "
                            + source.catalog());
        }
        return provider;
    }

    /**
     * Rejects a provider whose capability format this kernel does not understand.
     *
     * <p>Validation runs on the first pin of a catalog/connector pair instead of
     * on unpin, where the source table may already be gone.
     */
    private void requireCompatibleCapabilities(SourceProvider provider, SourceRef source) {
        String key = source.catalog() + '\u0000' + provider.connectorId();
        if (!validatedCapabilities.add(key)) {
            return;
        }
        try {
            provider.capabilities(source).requireCompatibleVersion();
        } catch (RuntimeException failure) {
            validatedCapabilities.remove(key);
            throw failure;
        }
    }

    private MaterializationCapabilities capabilities(SourceProvider provider, SourceRef source) {
        MaterializationCapabilities capabilities = provider.capabilities(source);
        capabilities.requireCompatibleVersion();
        validatedCapabilities.add(source.catalog() + '\u0000' + provider.connectorId());
        return capabilities;
    }
}
