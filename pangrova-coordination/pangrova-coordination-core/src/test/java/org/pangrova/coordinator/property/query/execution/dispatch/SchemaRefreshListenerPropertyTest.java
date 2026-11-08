package org.pangrova.coordinator.property.query.execution.dispatch;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;

import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementCompletionEvent;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.listener.SchemaRefreshListener;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for SchemaRefreshListener trigger behavior.
 *
 * <p><b>Validates: Requirement 3.3</b></p>
 */
class SchemaRefreshListenerPropertyTest {

    private static final Statement DUMMY_STMT = new CreateTableStatement(
            Identifier.of("c", "s", "t"), List.of(), false,
            List.of(), List.of(), Optional.empty(), Map.of());

    /**
     * Property 9: DDL/DML events trigger compilerRefreshSupplier.
     */
    @Property(tries = 30)
    void ddlOrDmlEventTriggersRefresh(@ForAll("ddlOrDmlCategories") StatementCategory category) {
        AtomicInteger refreshCount = new AtomicInteger(0);
        AtomicInteger updateCount = new AtomicInteger(0);

        SchemaRefreshListener listener = new SchemaRefreshListener(
                () -> { refreshCount.incrementAndGet(); return null; },
                compiler -> updateCount.incrementAndGet());

        StatementResult result = new StatementResult(
                List.of(), List.of(), "cat.sch.tbl", category);
        StatementCompletionEvent event = new StatementCompletionEvent(
                "q-1", DUMMY_STMT, result, null);

        listener.onCompleted(event);

        assertThat(refreshCount.get()).isEqualTo(1);
        assertThat(updateCount.get()).isEqualTo(1);
    }

    /**
     * Property 9 (continued): Non-DDL/DML events do NOT trigger refresh.
     */
    @Property(tries = 30)
    void nonDdlDmlEventDoesNotTriggerRefresh(
            @ForAll("nonDdlDmlCategories") StatementCategory category) {
        AtomicInteger refreshCount = new AtomicInteger(0);
        AtomicInteger updateCount = new AtomicInteger(0);

        SchemaRefreshListener listener = new SchemaRefreshListener(
                () -> { refreshCount.incrementAndGet(); return null; },
                compiler -> updateCount.incrementAndGet());

        StatementResult result = new StatementResult(
                List.of(), List.of(), null, category);
        StatementCompletionEvent event = new StatementCompletionEvent(
                "q-1", DUMMY_STMT, result, null);

        listener.onCompleted(event);

        assertThat(refreshCount.get()).isEqualTo(0);
        assertThat(updateCount.get()).isEqualTo(0);
    }

    @Provide
    Arbitrary<StatementCategory> ddlOrDmlCategories() {
        return Arbitraries.of(
                StatementCategory.DDL, StatementCategory.DML,
                StatementCategory.DYNAMIC_TABLE, StatementCategory.INDEX,
                StatementCategory.CLUSTERING, StatementCategory.STREAM);
    }

    @Provide
    Arbitrary<StatementCategory> nonDdlDmlCategories() {
        return Arbitraries.of(
                StatementCategory.EXPLAIN, StatementCategory.SHOW,
                StatementCategory.MAINTENANCE, StatementCategory.TENANT);
    }
}
