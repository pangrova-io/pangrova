package org.pangrova.coordinator.materialization;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.pangrova.materialization.domain.QueryId;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.materialization.usecase.ReaderPinLeases;
import org.pangrova.connector.api.session.ConnectorSession;

/**
 * Binds publication reads to the actual query's issuance and durable reader leases.
 *
 * <p>Construction does not require a query: schema discovery and ordinary table planning may
 * happen before admission. Exposing a published generation does require both the lease service
 * and an open, engine-issued query authority. Session properties cannot provide that authority.
 * The operation permit prevents terminal cleanup from releasing pins while acquisition is still
 * in flight. The query lifecycle must use the same leases instance for renewal and release.
 * This adapter borrows both authorities and owns neither service.
 */
public final class MaterializationReadPin implements Consumer<PublishedMaterialization> {

    private final Optional<ReaderPinLeases> leases;
    private final ConnectorSession session;

    public MaterializationReadPin(Optional<ReaderPinLeases> leases, ConnectorSession session) {
        this.leases = Objects.requireNonNull(leases, "leases");
        this.session = Objects.requireNonNull(session, "session");
    }

    @Override
    public void accept(PublishedMaterialization published) {
        Objects.requireNonNull(published, "published");
        ReaderPinLeases service = leases.orElseThrow(() -> new IllegalStateException(
                "Published materialization reads require a reader pin lease service"));
        var authority = session.getQueryAuthority().orElseThrow(() -> new IllegalStateException(
                "Published materialization reads require an admitted query authority"));
        try (var operation = authority.enterOperation()) {
            service.acquire(published.generation(), new QueryId(authority.queryId()), session.getUser());
        }
    }
}
