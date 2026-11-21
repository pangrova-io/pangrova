package org.pangrova.connector.lance;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.NativeDatasetControlPlane;

import java.security.MessageDigest;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

/** Resolves durable historical metadata only after Native proves its exact physical manifest. */
final class LanceSnapshotResolver {
    private final Supplier<LanceCatalogStore> store;
    private final Optional<NativeDatasetControlPlane> control;
    private final String catalog;
    private final String warehouse;

    LanceSnapshotResolver(Supplier<LanceCatalogStore> store, Optional<NativeDatasetControlPlane> control,
            String catalog, String warehouse) {
        this.store = Objects.requireNonNull(store);
        this.control = Objects.requireNonNull(control);
        this.catalog = Objects.requireNonNull(catalog);
        this.warehouse = Objects.requireNonNull(warehouse);
    }

    LanceTableHandle resolve(ConnectorSession session, LanceTableHandle selector) {
        if (!catalog.equals(selector.catalog()) || !selector.datasetUri().startsWith(warehouse + "/")) {
            throw new IllegalArgumentException("Lance snapshot belongs to another catalog or warehouse");
        }
        LanceSessionIdentity.requireTenant(session, selector.tenantId());
        var current = store.get().load(selector.catalog(), selector.tenantId(), selector.table());
        if (current == null || !current.tableId().equals(selector.tableId())
                || !current.datasetUri().equals(selector.datasetUri())) {
            throw new IllegalStateException("Lance snapshot table incarnation is no longer visible");
        }
        if (selector.version() == 0) {
            // EMPTY is a selected snapshot, not a request to refresh to the latest publication.
            // The incarnation check above still rejects a dropped or replaced table.
            return selector;
        }
        var evidence = store.get().loadSnapshot(selector);
        var historical = evidence.table();
        requireHistoricalIdentity(historical, selector);
        var page = control.orElseThrow(() -> new IllegalStateException("Lance snapshot requires Native control"))
                .scanPartitions(session, evidence.reference(), OptionalLong.empty(), 1);
        if (page.version() != historical.version()
                || !MessageDigest.isEqual(page.schemaFingerprint(), historical.schemaFence())
                || !MessageDigest.isEqual(page.manifestSha256(), evidence.manifestDigest())) {
            throw new IllegalStateException("Lance Native manifest differs from historical publication evidence");
        }
        return historical;
    }

    private static void requireHistoricalIdentity(
            LanceTableHandle historical, LanceTableHandle selector) {
        if (!historical.catalog().equals(selector.catalog())
                || !historical.tenantId().equals(selector.tenantId())
                || !historical.table().equals(selector.table())
                || !historical.tableId().equals(selector.tableId())
                || !historical.datasetUri().equals(selector.datasetUri())
                || historical.version() != selector.version()) {
            throw new IllegalStateException("Lance historical publication identity mismatch");
        }
    }
}
