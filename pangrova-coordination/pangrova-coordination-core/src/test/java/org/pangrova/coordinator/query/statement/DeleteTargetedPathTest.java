package org.pangrova.coordinator.query.statement;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.dispatch.*;
import org.pangrova.query.core.statement.StatementTableTargets;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.parser.api.ast.*;
import org.pangrova.parser.api.ast.expression.BinaryExpression;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies DELETE statement routing through StatementDispatcher.
 *
 * <p>In the 2-Factory architecture, DELETE goes through PlanExecutionFactory
 * (native worker path). This test verifies that DELETE statements are correctly
 * routable and that the dispatcher fires completion events.</p>
 */
class DeleteTargetedPathTest {

    private static final QuerySession TEST_SESSION =
            new QuerySession("q-delete", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

    @Test
    void deleteIsRoutableThroughDispatcher() {
        StatementDispatcher dispatcher = buildDeleteDispatcher(new ArrayList<>());
        DeleteStatement delete = buildDelete(2);
        assertThat(dispatcher.isRoutable(delete)).isTrue();
    }

    @Test
    void deleteExecutionFiresCompletionEvent() {
        List<StatementCompletionEvent> events = new CopyOnWriteArrayList<>();
        StatementDispatcher dispatcher = buildDeleteDispatcher(events);

        dispatcher.executeDirect(buildDelete(2), null, "q-del-1", TEST_SESSION);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).queryId()).isEqualTo("q-del-1");
    }

    @Test
    void deleteWithDifferentTargetIdsAllRoutable() {
        StatementDispatcher dispatcher = buildDeleteDispatcher(new ArrayList<>());
        for (int id : List.of(1, 2, 99, Integer.MAX_VALUE)) {
            assertThat(dispatcher.isRoutable(buildDelete(id)))
                    .as("DELETE with target id=%d must be routable", id)
                    .isTrue();
        }
    }

    @Test
    void deleteCompletionEventContainsStatement() {
        List<StatementCompletionEvent> events = new CopyOnWriteArrayList<>();
        StatementDispatcher dispatcher = buildDeleteDispatcher(events);
        DeleteStatement delete = buildDelete(42);

        dispatcher.executeDirect(delete, null, "q-del-stmt", TEST_SESSION);

        assertThat(events.get(0).statement()).isSameAs(delete);
    }

    @Test
    void deleteResultCategoryIsDml() {
        List<StatementCompletionEvent> events = new CopyOnWriteArrayList<>();
        StatementDispatcher dispatcher = buildDeleteDispatcher(events);

        dispatcher.executeDirect(buildDelete(1), null, "q-del-cat", TEST_SESSION);

        assertThat(events.get(0).result().category()).isEqualTo(StatementCategory.DML);
    }

    // ── Helpers ──

    private static DeleteStatement buildDelete(int targetId) {
        return new DeleteStatement(
                Identifier.of("mem", "default", "orders"),
                Optional.of(new BinaryExpression(
                        Identifier.of("id"),
                        BinaryExpression.Operator.EQUAL,
                        Literal.ofInteger(targetId))));
    }

    private static StatementDispatcher buildDeleteDispatcher(
            List<StatementCompletionEvent> events) {
        MetadataTask<Statement> deleteTask = (stmt, ctx) -> new StatementResult(
                List.of(), List.of(), java.util.Optional.ofNullable(StatementTableTargets.identifier(stmt))
                .map(id -> String.join(".", id.parts())).orElse(null), StatementCategory.DML);
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(DeleteStatement.class, deleteTask);
        var factory = new MetadataExecutionFactory(tasks);
        StatementCompletionListener listener = events::add;
        var context = new StatementDispatcherContext(
                null, null, List.of(factory), List.of(listener), null);
        return new StatementDispatcher(context);
    }
}
