package org.pangrova.coordinator.materialization;

import java.util.Objects;
import javax.sql.DataSource;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.task.CtasTask;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.port.PublicationVerifier;
import org.pangrova.materialization.usecase.KernelPorts;
import org.pangrova.materialization.usecase.MaterializationService;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.materialization.JdbcMaterializationCatalog;
import org.pangrova.metadata.materialization.JdbcMaterializationRepository;
import org.pangrova.spi.security.AccessControl;

/**
 * Builds kernel collaborators bound to one coordinator statement.
 *
 * <p>The planner, executor, fence, and authorizer all observe the exact query
 * identity, session, tenant, and execution anchor of the current statement.
 * There is no ambient or cached command identity.
 */
public final class MaterializationKernelFactory {

    private final JdbcMaterializationRepository repository;
    private final JdbcMaterializationCatalog catalog;
    private final CatalogRegistry catalogs;
    private final HighAvailabilityCoordinationService coordination;
    private final CtasTask ctas;
    private final DataSource metadataDataSource;
    private final AccessControl accessControl;

    public MaterializationKernelFactory(JdbcMaterializationRepository repository,
            JdbcMaterializationCatalog catalog, CatalogRegistry catalogs,
            HighAvailabilityCoordinationService coordination, CtasTask ctas,
            DataSource metadataDataSource, AccessControl accessControl) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        this.ctas = Objects.requireNonNull(ctas, "ctas");
        this.metadataDataSource = Objects.requireNonNull(metadataDataSource, "metadataDataSource");
        this.accessControl = accessControl;
    }

    public MaterializationService service(StatementExecutionContext context) {
        TenantId tenant = tenant(context);
        CoordinatorExecutionFence fence = new CoordinatorExecutionFence(
                coordination, context.queryId(), coordination::getActiveLeaderEpoch);
        CoordinatorMaterializationPlanner planner = new CoordinatorMaterializationPlanner(
                context.compiler(), context.session(), catalogs, catalog, tenant);
        return new MaterializationService(new KernelPorts(
                repository,
                catalog,
                planner,
                new NativeCtasExecutionEngine(context, catalogs, ctas, metadataDataSource),
                new CoordinatorSourcePinning(catalogs),
                new CoordinatorMaterializationAuthorizer(context.session(), accessControl),
                fence,
                fence,
                new NativeCommitPublicationVerifier(metadataDataSource),
                MaterializationExtensions.standard(),
                new UuidIdGenerator(),
                new CoordinatorClock()));
    }

    public JdbcMaterializationRepository repository() {
        return repository;
    }

    public JdbcMaterializationCatalog catalog() {
        return catalog;
    }

    public CatalogRegistry catalogs() {
        return catalogs;
    }

    public DataSource metadataDataSource() {
        return metadataDataSource;
    }

    public CoordinatorExecutionFence fence(String queryId) {
        return new CoordinatorExecutionFence(
                coordination, queryId, coordination::getActiveLeaderEpoch);
    }

    public PublicationVerifier verifier() {
        return new NativeCommitPublicationVerifier(metadataDataSource);
    }

    public org.pangrova.materialization.port.SourcePinning sourcePinning() {
        return new CoordinatorSourcePinning(catalogs);
    }

    public Clock clock() {
        return new CoordinatorClock();
    }

    public static TenantId tenant(StatementExecutionContext context) {
        String tenant = context.session().tenantId();
        return new TenantId(tenant == null || tenant.isBlank() ? "default" : tenant);
    }

    public static Principal principal(StatementExecutionContext context) {
        if (context.session().userIdentity() == null) {
            throw MaterializationException.requestRejected(
                    "materialization commands require an authenticated principal");
        }
        return new Principal(context.session().userIdentity().principal());
    }
}
