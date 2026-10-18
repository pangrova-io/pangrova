package org.pangrova.coordinator.dynamictable;

import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.task.CtasTask;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.metadata.dynamictable.DynamicTablePublication;
import org.pangrova.metadata.dynamictable.JdbcDynamicTablePublicationStore;
import org.pangrova.parser.AstPrinter;
import org.pangrova.parser.ast.CreateDynamicTableStatement;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.RefreshMode;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.AlterDynamicTableAction;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.ast.statement.CreateTableAsSelectStatement;
import org.pangrova.parser.ast.statement.ShowDynamicTablesStatement;
import org.pangrova.spi.connector.data.TypedRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/** Foreground full-refresh Dynamic Tables backed by Native CTAS and an atomic JDBC head. */
public final class DynamicTableMaterializationService {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableMaterializationService.class);
    private static final Pattern NAME_PART = Pattern.compile("[A-Za-z_][A-Za-z_0-9]*");

    private final JdbcDynamicTablePublicationStore store;
    private final CtasTask ctas;

    /** Creates the production materializer with a durable publication authority. */
    public DynamicTableMaterializationService(JdbcDynamicTablePublicationStore store,
            CtasTask ctas) {
        this.store = Objects.requireNonNull(store, "store");
        this.ctas = Objects.requireNonNull(ctas, "ctas");
    }

    /** Creates and publishes the first physical generation before reporting success. */
    public StatementResult create(CreateDynamicTableStatement statement,
            StatementExecutionContext context) {
        requireManualSnapshotMode(statement);
        String logicalName = requireQualifiedName(statement.name());
        String tenantId = tenantId(context);
        String definitionSql = new AstPrinter().print(statement.query());
        store.create(tenantId, logicalName, definitionSql,
                context.session().sessionProperties().get("catalog"),
                context.session().sessionProperties().get("schema"));
        DynamicTablePublication expected = store.load(tenantId, logicalName).orElseThrow();
        Identifier candidate = candidateTable(statement.name());
        try {
            materialize(candidate, statement.query(), context);
        } catch (RuntimeException failure) {
            // A timeout does not prove Native writers have stopped. Retain the
            // isolated candidate until a fenced, task-drained GC can prove safety.
            try {
                store.abandonCreation(expected);
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        // A lost JDBC acknowledgement may follow a committed head update. Do not
        // delete the candidate if publication throws: it may already be visible.
        store.publish(expected, String.join(".", candidate.parts()));
        LOG.info("event=dynamic_table_published queryId={} logicalName={} revision=1",
                context.queryId(), logicalName);
        return result("Dynamic Table " + logicalName + " created", logicalName);
    }

    /** Recomputes into an isolated table and publishes only after Native commit. */
    public StatementResult alter(AlterDynamicTableStatement statement,
            StatementExecutionContext context) {
        if (!(statement.action() instanceof AlterDynamicTableAction.Refresh)) {
            throw new UnsupportedOperationException(
                    "Only manual full REFRESH is supported by the materialized Dynamic Table runtime");
        }
        String logicalName = requireQualifiedName(statement.name());
        DynamicTablePublication expected = requireActive(tenantId(context), logicalName);
        Statement parsed = context.compiler().parseToAst(expected.definitionSql());
        if (!(parsed instanceof Query query)) {
            throw new IllegalStateException("Persisted Dynamic Table definition is not a SELECT: " + logicalName);
        }
        Identifier candidate = candidateTable(statement.name());
        materialize(candidate, query, withDefinitionBindings(context, expected));
        store.publish(expected, String.join(".", candidate.parts()));
        LOG.info("event=dynamic_table_published queryId={} logicalName={} revision={}",
                context.queryId(), logicalName, expected.revision() + 1);
        return result("Dynamic Table " + logicalName + " refreshed", logicalName);
    }

    /** Removes the logical name; physical generations are retained for pinned readers. */
    public StatementResult drop(DropDynamicTableStatement statement,
            StatementExecutionContext context) {
        if (statement.cascade()) {
            throw new UnsupportedOperationException("Dynamic Table CASCADE requires durable dependency publication");
        }
        String logicalName = requireQualifiedName(statement.name());
        DynamicTablePublication expected = store.load(tenantId(context), logicalName)
                .orElseThrow(() -> new IllegalStateException("Dynamic Table not found: " + logicalName));
        store.delete(expected);
        return result("Dynamic Table " + logicalName + " dropped", logicalName);
    }

    /** Lists the tenant's persisted Dynamic Table publication heads. */
    public StatementResult show(ShowDynamicTablesStatement statement,
            StatementExecutionContext context) {
        List<TypedRow> rows = store.list(tenantId(context)).stream()
                .map(publication -> TypedRow.builder()
                        .putString("dt_name", publication.logicalName())
                        .putString("tenant_id", publication.tenantId())
                        .putString("state", publication.state().name())
                        .putString("refresh_mode", "SNAPSHOT_FULL_MANUAL")
                        .putLong("total_refresh_count", publication.revision())
                        .build())
                .toList();
        return new StatementResult(rows, List.of("dt_name", "tenant_id", "state",
                "refresh_mode", "total_refresh_count"), null, StatementCategory.DYNAMIC_TABLE);
    }

    /** Describes one tenant-scoped Dynamic Table and its currently published head. */
    public StatementResult describe(DescribeDynamicTableStatement statement,
            StatementExecutionContext context) {
        String logicalName = requireQualifiedName(statement.name());
        DynamicTablePublication publication = store.load(tenantId(context), logicalName)
                .orElseThrow(() -> new IllegalStateException("Dynamic Table not found: " + logicalName));
        TypedRow row = TypedRow.builder()
                .putString("dt_name", publication.logicalName())
                .putString("tenant_id", publication.tenantId())
                .putString("state", publication.state().name())
                .putString("refresh_mode", "SNAPSHOT_FULL_MANUAL")
                .putString("published_table", publication.publishedTable() == null
                        ? "" : publication.publishedTable())
                .putLong("revision", publication.revision())
                .build();
        return new StatementResult(List.of(row), List.copyOf(row.columnNames()),
                null, StatementCategory.DYNAMIC_TABLE);
    }

    private void materialize(Identifier candidate, Query query,
            StatementExecutionContext context) {
        var state = Objects.requireNonNull(context.stateMachine(), "Dynamic Table query state");
        state.deferTerminalUntilControlPlaneResult();
        ctas.execute(new CreateTableAsSelectStatement(candidate, false, query), context);
        if (state.nativeWriteCompletionReady().getNow(
                org.pangrova.coordinator.query.QueryStateMachine.State.FAILED)
                != org.pangrova.coordinator.query.QueryStateMachine.State.FINISHED) {
            throw new IllegalStateException("Native materialization did not finish before publication");
        }
    }

    private DynamicTablePublication requireActive(String tenantId, String logicalName) {
        DynamicTablePublication publication = store.load(tenantId, logicalName)
                .orElseThrow(() -> new IllegalStateException("Dynamic Table not found: " + logicalName));
        if (publication.state() != DynamicTablePublication.State.ACTIVE) {
            throw new IllegalStateException("Dynamic Table has no published generation: " + logicalName);
        }
        return publication;
    }

    private static Identifier candidateTable(Identifier logicalName) {
        List<String> parts = logicalName.parts();
        String candidate = "__pg_dt_" + UUID.randomUUID().toString().replace("-", "");
        return Identifier.of(parts.get(0).toLowerCase(Locale.ROOT),
                parts.get(1).toLowerCase(Locale.ROOT), candidate);
    }

    static StatementExecutionContext withDefinitionBindings(
            StatementExecutionContext context, DynamicTablePublication publication) {
        CoordinatorSessionContext session = context.session();
        Map<String, String> properties = new HashMap<>(session.sessionProperties());
        properties.remove("catalog");
        properties.remove("schema");
        if (publication.bindingCatalog() != null) {
            properties.put("catalog", publication.bindingCatalog());
        }
        if (publication.bindingSchema() != null) {
            properties.put("schema", publication.bindingSchema());
        }
        CoordinatorSessionContext bound = new CoordinatorSessionContext(
                session.queryId(), properties, session.userIdentity(), session.tenantId(),
                session.queryAuthority(), session.traceContext());
        return new StatementExecutionContext(context.stateMachine(), context.queryId(),
                bound, context.compiler(), context.resultService());
    }

    private static String requireQualifiedName(Identifier name) {
        if (name.parts().size() != 3
                || name.parts().stream().anyMatch(part -> !NAME_PART.matcher(part).matches())) {
            throw new IllegalArgumentException(
                    "Materialized Dynamic Table names must be unquoted catalog.schema.table identifiers");
        }
        return String.join(".", name.parts()).toLowerCase(Locale.ROOT);
    }

    static void requireManualSnapshotMode(CreateDynamicTableStatement statement) {
        if (!statement.targetLag().isZero()) {
            throw new UnsupportedOperationException(
                    "Scheduled TARGET_LAG refresh is not implemented; use TARGET_LAG = 'PT0S' "
                    + "for manual full refresh with ALTER DYNAMIC TABLE ... REFRESH");
        }
        if (statement.refreshMode().orElse(RefreshMode.AUTO) == RefreshMode.CDC) {
            throw new UnsupportedOperationException("CDC Dynamic Table refresh is not yet materialized");
        }
        if (statement.warehouse().isPresent() || !statement.properties().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Dynamic Table WAREHOUSE and WITH properties require a materialization policy");
        }
    }

    private static String tenantId(StatementExecutionContext context) {
        String tenant = context.session().tenantId();
        return tenant == null || tenant.isBlank() ? "default" : tenant;
    }

    private static StatementResult result(String message, String logicalName) {
        TypedRow row = TypedRow.builder().putString("result", message).build();
        return new StatementResult(List.of(row), List.of("result"),
                logicalName, StatementCategory.DYNAMIC_TABLE);
    }
}
