package org.pangrova.coordinator.materialization;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.persistence.materialization.api.publication.MaterializationCatalog;
import org.pangrova.persistence.materialization.api.publication.MaterializationPublicationSnapshot;

/** One tenant publication directory shared by the planning paths of an admitted query. */
@ThreadSafe
public final class PlanningMaterializations {
    /** Reserved session value installed from the trusted query admission grant. */
    public static final String ADMISSION_REVISION_PROPERTY = "pangrova.materialization.publication-revision";

    private final MaterializationCatalog catalog;
    private final TenantId tenant;
    private final OptionalLong admissionRevision;
    private final AtomicBoolean started = new AtomicBoolean();
    private final CompletableFuture<MaterializationPublicationSnapshot> snapshot = new CompletableFuture<>();

    /** Opens a lazy planning directory for this tenant using an authenticated admission revision. */
    public PlanningMaterializations(MaterializationCatalog catalog, String tenant, OptionalLong admissionRevision) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.tenant = new TenantId(tenant);
        this.admissionRevision = Objects.requireNonNull(admissionRevision, "admissionRevision");
    }

    /** Binds the directory to an authenticated connector session. */
    public static PlanningMaterializations forSession(MaterializationCatalog catalog,
            ConnectorSession session, String defaultTenantId) {
        var properties = session.getProperties();
        String revision = properties.get(ADMISSION_REVISION_PROPERTY);
        return new PlanningMaterializations(catalog, properties.getOrDefault("tenant_id", defaultTenantId),
                revision == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(revision)));
    }

    /** Resolves the snapshot once for both direct name resolution and transparent rewrite. */
    public MaterializationPublicationSnapshot snapshot() {
        if (started.compareAndSet(false, true)) {
            try {
                snapshot.complete(catalog.snapshot(tenant, admissionRevision));
            } catch (RuntimeException | Error failure) {
                snapshot.completeExceptionally(failure);
                throw failure;
            }
        }
        try {
            return snapshot.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure.getCause() instanceof Error error) {
                throw error;
            }
            throw failure;
        }
    }
}
