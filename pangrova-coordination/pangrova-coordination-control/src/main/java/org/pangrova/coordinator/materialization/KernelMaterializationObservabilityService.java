package org.pangrova.coordinator.materialization;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.materialization.api.domain.Dependency;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.materialization.api.port.GcHistoryEntry;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.ShowMaterializationDependenciesStatement;
import org.pangrova.parser.api.ast.statement.ShowMaterializationGcHistoryStatement;
import org.pangrova.parser.api.ast.statement.ShowMaterializationRefreshHistoryStatement;
import org.pangrova.parser.api.ast.statement.ShowMaterializationsStatement;
import org.pangrova.types.value.TypedRow;
import org.pangrova.security.api.authorization.AccessControl;

/**
 * Read-only system surfaces over the durable materialization kernel.
 *
 * <p>Every projection is tenant-scoped and served from metadata only; no
 * physical generation is opened and no object definition is recompiled.
 */
public final class KernelMaterializationObservabilityService {

    private static final int HISTORY_LIMIT = 1000;

    private static final List<String> OBJECT_COLUMNS = List.of(
            "name", "kind", "state", "revision", "definition_revision",
            "published_generation", "definer", "security_mode", "refresh_mode",
            "incremental_strategy", "created_at", "updated_at");

    private static final List<String> ATTEMPT_COLUMNS = List.of(
            "attempt_id", "object", "refresh_kind", "trigger", "state", "error_code",
            "definition_revision", "requesting", "definer", "created_at", "updated_at");

    private static final List<String> DEPENDENCY_COLUMNS = List.of(
            "downstream", "downstream_definition_revision", "upstream", "kind");

    private static final List<String> GC_COLUMNS = List.of(
            "object", "generation", "decision", "state", "evidence",
            "created_at", "updated_at");

    private final MaterializationRepository repository;
    private final AccessControl accessControl;

    /**
     * Creates the observability read model.
     *
     * @param accessControl current object access policy
     * @param repository durable materialization repository
     */
    public KernelMaterializationObservabilityService(MaterializationRepository repository, AccessControl accessControl) {
        this.accessControl = Objects.requireNonNull(accessControl, "accessControl");
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    /**
     * Lists materialization objects of the authenticated tenant.
     *
     * @param statement SHOW statement
     * @param context statement execution context
     * @return result rows
     */
    public StatementResult listObjects(ShowMaterializationsStatement statement,
            StatementExecutionContext context) {
        var authorizer = new CoordinatorMaterializationAuthorizer(context.session(), accessControl);
        List<TypedRow> rows = repository.listObjects(tenant(context)).stream()
                .filter(object -> authorizer.mayRead(object.id()))
                .map(KernelMaterializationObservabilityService::objectRow)
                .toList();
        return new StatementResult(rows, OBJECT_COLUMNS, null, StatementCategory.SHOW);
    }

    /**
     * Lists refresh attempts of one object or the whole tenant.
     *
     * @param statement SHOW statement
     * @param context statement execution context
     * @return result rows
     */
    public StatementResult listRefreshHistory(
            ShowMaterializationRefreshHistoryStatement statement,
            StatementExecutionContext context) {
        var authorizer = new CoordinatorMaterializationAuthorizer(context.session(), accessControl);
        List<TypedRow> rows = repository.findAttempts(tenant(context),
                        scoped(statement.object(), context, authorizer), HISTORY_LIMIT).stream()
                .filter(attempt -> authorizer.mayRead(attempt.object()))
                .map(KernelMaterializationObservabilityService::attemptRow)
                .toList();
        return new StatementResult(rows, ATTEMPT_COLUMNS, null, StatementCategory.SHOW);
    }

    /**
     * Lists recorded dependency edges.
     *
     * @param statement SHOW statement
     * @param context statement execution context
     * @return result rows
     */
    public StatementResult listDependencies(
            ShowMaterializationDependenciesStatement statement,
            StatementExecutionContext context) {
        var authorizer = new CoordinatorMaterializationAuthorizer(context.session(), accessControl);
        List<TypedRow> rows = repository.findDependencies(tenant(context),
                        scoped(statement.object(), context, authorizer)).stream()
                .filter(edge -> authorizer.mayRead(edge.downstream()) && authorizer.mayRead(edge.upstream()))
                .map(KernelMaterializationObservabilityService::dependencyRow)
                .toList();
        return new StatementResult(rows, DEPENDENCY_COLUMNS, null, StatementCategory.SHOW);
    }

    /**
     * Lists garbage-collection decisions.
     *
     * @param statement SHOW statement
     * @param context statement execution context
     * @return result rows
     */
    public StatementResult listGcHistory(ShowMaterializationGcHistoryStatement statement,
            StatementExecutionContext context) {
        var authorizer = new CoordinatorMaterializationAuthorizer(context.session(), accessControl);
        List<TypedRow> rows = repository.findGcHistory(tenant(context),
                        scoped(statement.object(), context, authorizer), HISTORY_LIMIT).stream()
                .filter(entry -> authorizer.mayRead(entry.object()))
                .map(KernelMaterializationObservabilityService::gcRow)
                .toList();
        return new StatementResult(rows, GC_COLUMNS, null, StatementCategory.SHOW);
    }

    private static TenantId tenant(StatementExecutionContext context) {
        return MaterializationKernelFactory.tenant(context);
    }

    private static Optional<MaterializationId> scoped(Optional<Identifier> name,
            StatementExecutionContext context, CoordinatorMaterializationAuthorizer authorizer) {
        return name.map(value -> {
            MaterializationId object = MaterializationNames.object(value, context, "Materialization");
            authorizer.requireRead(object, MaterializationKernelFactory.principal(context));
            return object;
        });
    }

    private static TypedRow objectRow(MaterializationObject object) {
        return TypedRow.builder()
                .putString("name", object.id().name().toString())
                .putString("kind", object.kind().name())
                .putString("state", object.state().stateName())
                .putLong("revision", object.revision())
                .putLong("definition_revision", object.definitionRevision())
                .putString("published_generation", object.publishedGeneration()
                        .map(generation -> generation.value().toString()).orElse(""))
                .putString("definer", object.definer().name())
                .putString("security_mode", object.securityMode().name())
                .putString("refresh_mode", object.refreshMode().name())
                .putString("incremental_strategy", object.incrementalStrategy()
                        .map(strategy -> strategy.value()).orElse(""))
                .putString("created_at", object.createdAt().toString())
                .putString("updated_at", object.updatedAt().toString())
                .build();
    }

    private static TypedRow attemptRow(RefreshAttempt attempt) {
        return TypedRow.builder()
                .putString("attempt_id", attempt.id().value().toString())
                .putString("object", attempt.object().name().toString())
                .putString("refresh_kind", attempt.refreshKind().strategyId().value())
                .putString("trigger", attempt.trigger().name())
                .putString("state", attempt.state().stateName())
                .putString("error_code", attempt.errorCode().orElse(""))
                .putLong("definition_revision", attempt.definitionRevision())
                .putString("requesting", attempt.requesting().name())
                .putString("definer", attempt.definer().name())
                .putString("created_at", attempt.createdAt().toString())
                .putString("updated_at", attempt.updatedAt().toString())
                .build();
    }

    private static TypedRow dependencyRow(Dependency dependency) {
        return TypedRow.builder()
                .putString("downstream", dependency.downstream().name().toString())
                .putLong("downstream_definition_revision",
                        dependency.downstreamDefinitionRevision())
                .putString("upstream", dependency.upstream().name().toString())
                .putString("kind", dependency.kind().name())
                .build();
    }

    private static TypedRow gcRow(GcHistoryEntry entry) {
        return TypedRow.builder()
                .putString("object", entry.object().name().toString())
                .putString("generation", entry.generation().value().toString())
                .putString("decision", entry.decision().name())
                .putString("state", entry.state().name())
                .putString("evidence", entry.evidence())
                .putString("created_at", entry.createdAt().toString())
                .putString("updated_at", entry.updatedAt().toString())
                .build();
    }
}
