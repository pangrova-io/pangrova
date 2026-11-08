package org.pangrova.coordinator.materialization;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.materialization.usecase.CreateMaterialization;
import org.pangrova.materialization.usecase.DropMaterialization;
import org.pangrova.materialization.usecase.MaterializationService;
import org.pangrova.materialization.usecase.RefreshMaterialization;
import org.pangrova.materialization.usecase.RedefineMaterialization;
import org.pangrova.materialization.usecase.RenameMaterialization;
import org.pangrova.materialization.usecase.ResumeMaterialization;
import org.pangrova.materialization.usecase.SetRefreshMode;
import org.pangrova.materialization.usecase.SetTargetLag;
import org.pangrova.materialization.usecase.SuspendMaterialization;
import org.pangrova.parser.core.format.AstPrinter;
import org.pangrova.parser.api.ast.CreateDynamicTableStatement;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.AlterDynamicTableAction;
import org.pangrova.parser.api.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.api.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.parser.api.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.api.ast.statement.ShowDynamicTablesStatement;
import org.pangrova.types.value.TypedRow;

/**
 * Dynamic Table DDL and manual refresh served exclusively by the unified
 * materialization kernel.
 *
 * <p>The service never reads or writes a physical table directly: creation and
 * refresh reserve a kernel attempt, execute Native CTAS, and publish through
 * the durable head. Show and describe read only the published read model.
 */
public final class KernelDynamicTableService {

    private final MaterializationKernelFactory kernels;

    /**
     * Creates the kernel service adapter.
     *
     * @param kernels unified materialization kernel factory
     */
    public KernelDynamicTableService(MaterializationKernelFactory kernels) {
        this.kernels = Objects.requireNonNull(kernels, "kernels");
    }

    /**
     * Executes CREATE DYNAMIC TABLE.
     *
     * @param statement parsed statement
     * @param context statement execution context
     * @return result receipt
     */
    public StatementResult create(CreateDynamicTableStatement statement,
            StatementExecutionContext context) {
        requireSupportedDefinition(statement);
        MaterializationId object = object(statement.name(), context);
        RefreshMode refreshMode = refreshMode(statement);
        Principal principal = MaterializationKernelFactory.principal(context);
        kernels.service(context).create(new CreateMaterialization(
                object,
                MaterializationKind.DYNAMIC_TABLE,
                new AstPrinter().print(statement.query()),
                principal,
                principal,
                context.session().userIdentity().groups(),
                SecurityMode.DEFINER,
                refreshMode,
                incrementalStrategy(statement.incrementalStrategy()),
                targetLag(statement),
                boundProperties(context.session(), object),
                command(context),
                false));
        return result("Dynamic Table " + object.name() + " created", object.name().toString());
    }

    /**
     * Executes ALTER DYNAMIC TABLE.
     *
     * @param statement parsed statement
     * @param context statement execution context
     * @return result receipt
     */
    public StatementResult alter(AlterDynamicTableStatement statement,
            StatementExecutionContext context) {
        MaterializationId object = object(statement.name(), context);
        Principal principal = MaterializationKernelFactory.principal(context);
        MaterializationService service = kernels.service(context);
        var action = statement.action();
        if (action instanceof AlterDynamicTableAction.Refresh) {
            service.refresh(new RefreshMaterialization(
                    object, principal, TriggerKind.MANUAL, command(context)));
            return result("Dynamic Table " + object.name() + " refreshed",
                    object.name().toString());
        }
        if (action instanceof AlterDynamicTableAction.Suspend) {
            service.suspend(new SuspendMaterialization(
                    object, principal, command(context)));
            return result("Dynamic Table " + object.name() + " suspended",
                    object.name().toString());
        }
        if (action instanceof AlterDynamicTableAction.Resume) {
            service.resume(new ResumeMaterialization(
                    object, principal, command(context)));
            return result("Dynamic Table " + object.name() + " resumed",
                    object.name().toString());
        }
        if (action instanceof AlterDynamicTableAction.SetTargetLag setLag) {
            service.setTargetLag(new SetTargetLag(object, principal,
                    new TargetLag(setLag.newLag()), command(context)));
            return result("Dynamic Table " + object.name() + " target lag set to "
                    + setLag.newLag(), object.name().toString());
        }
        if (action instanceof AlterDynamicTableAction.SetRefreshMode setMode) {
            service.setRefreshMode(new SetRefreshMode(object, principal,
                    refreshMode(java.util.Optional.of(setMode.mode())),
                    java.util.Optional.empty(), command(context)));
            return result("Dynamic Table " + object.name() + " refresh mode set to "
                    + setMode.mode(), object.name().toString());
        }
        if (action instanceof AlterDynamicTableAction.Rename rename) {
            MaterializationId newObject = object(rename.newName(), context);
            service.rename(new RenameMaterialization(
                    object, principal, newObject, command(context)));
            return result("Dynamic Table " + object.name() + " renamed to "
                    + newObject.name(), newObject.name().toString());
        }
        if (action instanceof AlterDynamicTableAction.Redefine redefine) {
            service.redefine(new RedefineMaterialization(object, principal,
                    new AstPrinter().print(redefine.newQuery()),
                    boundProperties(context.session(), object), command(context)));
            return result("Dynamic Table " + object.name() + " redefined",
                    object.name().toString());
        }
        if (action instanceof AlterDynamicTableAction.BackfillEmbedding) {
            throw new UnsupportedOperationException(
                    "Dynamic Table BACKFILL EMBEDDING is not a materialization operation");
        }
        throw new UnsupportedOperationException(
                "Unsupported Dynamic Table action " + action.getClass().getSimpleName());
    }

    /**
     * Executes DROP DYNAMIC TABLE.
     *
     * @param statement parsed statement
     * @param context statement execution context
     * @return result receipt
     */
    public StatementResult drop(DropDynamicTableStatement statement,
            StatementExecutionContext context) {
        MaterializationId object = object(statement.name(), context);
        kernels.service(context).drop(new DropMaterialization(
                object, MaterializationKernelFactory.principal(context), command(context),
                statement.cascade()));
        return result("Dynamic Table " + object.name() + " dropped", object.name().toString());
    }

    /**
     * Executes SHOW DYNAMIC TABLES.
     *
     * @param statement parsed statement
     * @param context statement execution context
     * @return result rows
     */
    public StatementResult show(ShowDynamicTablesStatement statement,
            StatementExecutionContext context) {
        List<TypedRow> rows = kernels.catalog()
                .listPublished(MaterializationKernelFactory.tenant(context)).stream()
                .filter(published -> published.kind() == MaterializationKind.DYNAMIC_TABLE)
                .filter(published -> kernels.mayRead(context, published.object()))
                .map(published -> scheduleRow(published,
                        materialization(published.object()), schedule(published.object())).build())
                .toList();
        return new StatementResult(rows, List.of("dt_name", "tenant_id", "state",
                "refresh_mode", "schedule_state", "target_lag", "last_refresh_time",
                "revision"), null, StatementCategory.DYNAMIC_TABLE);
    }

    /**
     * Executes DESCRIBE DYNAMIC TABLE.
     *
     * @param statement parsed statement
     * @param context statement execution context
     * @return result row
     */
    public StatementResult describe(DescribeDynamicTableStatement statement,
            StatementExecutionContext context) {
        MaterializationId object = object(statement.name(), context);
        kernels.requireRead(context, object);
        PublishedMaterialization published = kernels.catalog().findPublished(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        MaterializationObject header = materialization(object);
        TypedRow row = scheduleRow(published, header, schedule(object))
                .putLong("definition_revision", header.definitionRevision())
                .putString("published_table", published.physicalTable().qualifiedName())
                .build();
        return new StatementResult(List.of(row), List.copyOf(row.columnNames()),
                null, StatementCategory.DYNAMIC_TABLE);
    }

    private MaterializationObject materialization(MaterializationId object) {
        return kernels.repository().findObject(object)
                .orElseThrow(() -> new IllegalStateException(
                        "materialization " + object.canonicalName() + " has no object header"));
    }

    private Schedule schedule(MaterializationId object) {
        return kernels.repository().findSchedule(object)
                .orElseThrow(() -> new IllegalStateException(
                        "materialization " + object.canonicalName() + " has no durable schedule"));
    }

    private static TypedRow.Builder scheduleRow(PublishedMaterialization published,
            MaterializationObject object, Schedule schedule) {
        return TypedRow.builder()
                .putString("dt_name", published.object().name().toString())
                .putString("tenant_id", published.object().tenant().value())
                .putString("state", object.state().stateName())
                .putString("refresh_mode", published.refreshKind().strategyId().value())
                .putString("schedule_state", schedule.state().name())
                .putString("target_lag", schedule.targetLag()
                        .map(lag -> lag.value().toString()).orElse(""))
                .putString("last_refresh_time", published.publishedAt().toString())
                .putLong("revision", published.definitionRevision());
    }

    static RefreshMode refreshMode(CreateDynamicTableStatement statement) {
        return refreshMode(statement.refreshMode());
    }

    static RefreshMode refreshMode(java.util.Optional<org.pangrova.parser.api.ast.RefreshMode> mode) {
        return mode
                .map(requested -> switch (requested) {
                    case SNAPSHOT, FULL -> RefreshMode.FULL;
                    case AUTO, INCREMENTAL -> RefreshMode.INCREMENTAL;
                    case CDC -> throw new UnsupportedOperationException(
                            "CDC refresh is not yet materialized");
                })
                .orElse(RefreshMode.FULL);
    }

    static java.util.Optional<StrategyId> incrementalStrategy(
            java.util.Optional<org.pangrova.parser.api.ast.IncrementalStrategy> strategy) {
        if (strategy.isEmpty()
                || strategy.get() == org.pangrova.parser.api.ast.IncrementalStrategy.AUTO) {
            return java.util.Optional.empty();
        }
        StrategyId resolved = switch (strategy.get()) {
            case AUTO -> throw new IllegalArgumentException(
                    "AUTO resolves to the unconfigured strategy");
            case APPEND_ONLY -> StrategyId.APPEND_ONLY;
            case PARTITION_REPLACE -> StrategyId.PARTITION_REPLACE;
            case AFFECTED_KEY -> StrategyId.AFFECTED_KEY;
            case CDC -> throw new UnsupportedOperationException(
                    "CDC incremental strategy is not yet materialized");
        };
        return java.util.Optional.of(resolved);
    }

    private static java.util.Optional<TargetLag> targetLag(CreateDynamicTableStatement statement) {
        Duration lag = statement.targetLag();
        return lag.isZero() ? java.util.Optional.empty()
                : java.util.Optional.of(new TargetLag(lag));
    }

    private static void requireSupportedDefinition(CreateDynamicTableStatement statement) {
        if (statement.warehouse().isPresent() || !statement.properties().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Dynamic Table WAREHOUSE and WITH properties require a materialization policy");
        }
    }

    private static MaterializationId object(Identifier name, StatementExecutionContext context) {
        return MaterializationNames.object(name, context, "Materialized Dynamic Table");
    }

    private static CommandId command(StatementExecutionContext context) {
        return new CommandId("materialization:" + context.queryId());
    }

    private static Map<String, String> boundProperties(QuerySession session,
            MaterializationId object) {
        Map<String, String> properties = new HashMap<>(session.sessionProperties());
        properties.remove("catalog");
        properties.remove("schema");
        properties.put("catalog", object.name().parts().get(0));
        properties.put("schema", object.name().parts().get(1));
        return properties;
    }

    private static StatementResult result(String message, String logicalName) {
        TypedRow row = TypedRow.builder().putString("result", message).build();
        return new StatementResult(List.of(row), List.of("result"),
                logicalName, StatementCategory.DYNAMIC_TABLE);
    }
}
