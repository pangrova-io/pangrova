package org.pangrova.coordinator.materialization;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.materialization.usecase.CreateMaterialization;
import org.pangrova.materialization.usecase.DropMaterialization;
import org.pangrova.materialization.usecase.MaterializationService;
import org.pangrova.materialization.usecase.RefreshMaterialization;
import org.pangrova.materialization.usecase.RenameMaterialization;
import org.pangrova.materialization.usecase.ResumeMaterialization;
import org.pangrova.materialization.usecase.SetRefreshMode;
import org.pangrova.materialization.usecase.SetTargetLag;
import org.pangrova.materialization.usecase.SuspendMaterialization;
import org.pangrova.parser.AstPrinter;
import org.pangrova.parser.ast.CreateMaterializedViewStatement;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.statement.AlterMaterializedViewAction;
import org.pangrova.parser.ast.statement.AlterMaterializedViewStatement;
import org.pangrova.parser.ast.statement.DescribeMaterializedViewStatement;
import org.pangrova.parser.ast.statement.DropMaterializedViewStatement;
import org.pangrova.parser.ast.statement.RefreshMaterializedViewStatement;
import org.pangrova.parser.ast.statement.ShowMaterializedViewsStatement;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Materialized View DDL served exclusively by the unified materialization
 * kernel.
 *
 * <p>A materialized view is a kernel object: {@code CREATE} publishes the first
 * generation, {@code REFRESH} recomputes and republishes, {@code ALTER} manages
 * scheduling without touching the definition, and show/describe read only the
 * published read model. Omitting {@code TARGET_LAG} means manual refresh; a
 * positive lag creates a durable schedule and {@code REFRESH_MODE = AUTO}
 * persists incremental refresh. No statement touches a physical table directly
 * or performs coordinator-side row processing.
 */
public final class KernelMaterializedViewService {

    private final MaterializationKernelFactory kernels;

    public KernelMaterializedViewService(MaterializationKernelFactory kernels) {
        this.kernels = Objects.requireNonNull(kernels, "kernels");
    }

    public StatementResult create(CreateMaterializedViewStatement statement,
            StatementExecutionContext context) {
        MaterializationId object = object(statement.name(), context);
        Principal principal = MaterializationKernelFactory.principal(context);
        MaterializationService service = kernels.service(context);
        CreateMaterialization command = new CreateMaterialization(
                object,
                MaterializationKind.MATERIALIZED_VIEW,
                new AstPrinter().print(statement.query()),
                principal,
                principal,
                SecurityMode.DEFINER,
                KernelDynamicTableService.refreshMode(statement.refreshMode()),
                KernelDynamicTableService.incrementalStrategy(statement.incrementalStrategy()),
                statement.targetLag().isZero()
                        ? java.util.Optional.empty()
                        : java.util.Optional.of(new TargetLag(statement.targetLag())),
                boundProperties(context.session(), object),
                command(context),
                statement.ifNotExists());
        if (statement.orReplace()) {
            service.createOrReplace(command);
        } else {
            service.create(command);
        }
        return result("Materialized View " + object.name() + " created",
                object.name().toString());
    }

    public StatementResult alter(AlterMaterializedViewStatement statement,
            StatementExecutionContext context) {
        MaterializationId object = object(statement.name(), context);
        Principal principal = MaterializationKernelFactory.principal(context);
        MaterializationService service = kernels.service(context);
        var action = statement.action();
        if (action instanceof AlterMaterializedViewAction.Refresh) {
            service.refresh(new RefreshMaterialization(
                    object, principal, TriggerKind.MANUAL, command(context)));
            return result("Materialized View " + object.name() + " refreshed",
                    object.name().toString());
        }
        if (action instanceof AlterMaterializedViewAction.Suspend) {
            service.suspend(new SuspendMaterialization(
                    object, principal, command(context)));
            return result("Materialized View " + object.name() + " suspended",
                    object.name().toString());
        }
        if (action instanceof AlterMaterializedViewAction.Resume) {
            service.resume(new ResumeMaterialization(
                    object, principal, command(context)));
            return result("Materialized View " + object.name() + " resumed",
                    object.name().toString());
        }
        if (action instanceof AlterMaterializedViewAction.SetTargetLag setLag) {
            service.setTargetLag(new SetTargetLag(object, principal,
                    new TargetLag(setLag.newLag()), command(context)));
            return result("Materialized View " + object.name() + " target lag set to "
                    + setLag.newLag(), object.name().toString());
        }
        if (action instanceof AlterMaterializedViewAction.SetRefreshMode setMode) {
            service.setRefreshMode(new SetRefreshMode(object, principal,
                    KernelDynamicTableService.refreshMode(java.util.Optional.of(setMode.mode())),
                    java.util.Optional.empty(), command(context)));
            return result("Materialized View " + object.name() + " refresh mode set to "
                    + setMode.mode(), object.name().toString());
        }
        if (action instanceof AlterMaterializedViewAction.Rename rename) {
            MaterializationId newObject = object(rename.newName(), context);
            service.rename(new RenameMaterialization(
                    object, principal, newObject, command(context)));
            return result("Materialized View " + object.name() + " renamed to "
                    + newObject.name(), newObject.name().toString());
        }
        throw new UnsupportedOperationException(
                "Unsupported Materialized View action "
                        + action.getClass().getSimpleName());
    }

    public StatementResult refresh(RefreshMaterializedViewStatement statement,
            StatementExecutionContext context) {
        MaterializationId object = object(statement.name(), context);
        Principal principal = MaterializationKernelFactory.principal(context);
        kernels.service(context).refresh(new RefreshMaterialization(
                object, principal, TriggerKind.MANUAL, command(context)));
        return result("Materialized View " + object.name() + " refreshed",
                object.name().toString());
    }

    public StatementResult drop(DropMaterializedViewStatement statement,
            StatementExecutionContext context) {
        MaterializationId object = object(statement.name(), context);
        kernels.service(context).drop(new DropMaterialization(
                object, MaterializationKernelFactory.principal(context), command(context),
                statement.cascade(), statement.ifExists()));
        return result("Materialized View " + object.name() + " dropped",
                object.name().toString());
    }

    public StatementResult show(ShowMaterializedViewsStatement statement,
            StatementExecutionContext context) {
        List<TypedRow> rows = kernels.catalog()
                .listPublished(MaterializationKernelFactory.tenant(context)).stream()
                .filter(published -> published.kind() == MaterializationKind.MATERIALIZED_VIEW)
                .filter(published -> matchesLike(
                        published.object().name().toString(), statement.pattern()))
                .map(published -> scheduleRow(published,
                        materialization(published.object()), schedule(published.object()))
                        .putString("published_table", published.physicalTable().qualifiedName())
                        .build())
                .toList();
        return new StatementResult(rows, List.of("mv_name", "tenant_id", "state",
                "refresh_mode", "schedule_state", "target_lag", "last_refresh_time",
                "published_table", "revision"), null, StatementCategory.DYNAMIC_TABLE);
    }

    public StatementResult describe(DescribeMaterializedViewStatement statement,
            StatementExecutionContext context) {
        MaterializationId object = object(statement.name(), context);
        PublishedMaterialization published = kernels.catalog().findPublished(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        TypedRow row = scheduleRow(published,
                materialization(object), schedule(object))
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
                .putString("mv_name", published.object().name().toString())
                .putString("tenant_id", published.object().tenant().value())
                .putString("state", object.state().stateName())
                .putString("refresh_mode", published.refreshKind().strategyId().value())
                .putString("schedule_state", schedule.state().name())
                .putString("target_lag", schedule.targetLag()
                        .map(lag -> lag.value().toString()).orElse(""))
                .putString("last_refresh_time", published.publishedAt().toString())
                .putLong("revision", published.definitionRevision());
    }

    private static boolean matchesLike(String value, String pattern) {
        if (pattern == null) {
            return true;
        }
        StringBuilder regex = new StringBuilder();
        int literalStart = 0;
        for (int i = 0; i < pattern.length(); i++) {
            char current = pattern.charAt(i);
            if (current == '%' || current == '_') {
                if (i > literalStart) {
                    regex.append(java.util.regex.Pattern.quote(pattern.substring(literalStart, i)));
                }
                regex.append(current == '%' ? ".*" : ".");
                literalStart = i + 1;
            }
        }
        if (literalStart < pattern.length()) {
            regex.append(java.util.regex.Pattern.quote(pattern.substring(literalStart)));
        }
        return value.matches(regex.toString());
    }

    private static MaterializationId object(Identifier name, StatementExecutionContext context) {
        return MaterializationNames.object(name, context, "Materialized View");
    }

    private static CommandId command(StatementExecutionContext context) {
        return new CommandId("materialization:" + context.queryId());
    }

    private static Map<String, String> boundProperties(CoordinatorSessionContext session,
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
