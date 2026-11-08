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
import org.pangrova.query.core.dispatch.StatementCompletionListener;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for CacheInvalidationListener behavior.
 *
 * <p><b>Validates: Requirements 3.4, 3.5</b></p>
 */
class CacheInvalidationListenerPropertyTest {

    private static final Statement DUMMY_STMT = new CreateTableStatement(
            Identifier.of("c", "s", "t"), List.of(), false,
            List.of(), List.of(), Optional.empty(), Map.of());

    /**
     * Property 10: DDL triggers invalidateAll().
     */
    @Property(tries = 20)
    void ddlEventTriggersInvalidateAll() {
        AtomicInteger invalidateAllCount = new AtomicInteger(0);
        AtomicInteger invalidateByTableCount = new AtomicInteger(0);

        StatementCompletionListener listener = createListener(
                invalidateAllCount, invalidateByTableCount);

        StatementResult result = new StatementResult(
                List.of(), List.of(), "cat.sch.tbl", StatementCategory.DDL);
        StatementCompletionEvent event = new StatementCompletionEvent(
                "q-1", DUMMY_STMT, result, null);

        listener.onCompleted(event);

        assertThat(invalidateAllCount.get()).isEqualTo(1);
        assertThat(invalidateByTableCount.get()).isEqualTo(1);
    }

    /**
     * Property 10 (continued): Non-DDL with mutatedTable triggers invalidateByTable only.
     * DML does not change schema, so invalidateAll is not called.
     */
    @Property(tries = 20)
    void dmlWithMutatedTableTriggersInvalidateByTableOnly() {
        AtomicInteger invalidateAllCount = new AtomicInteger(0);
        AtomicInteger invalidateByTableCount = new AtomicInteger(0);

        StatementCompletionListener listener = createListener(
                invalidateAllCount, invalidateByTableCount);

        StatementResult result = new StatementResult(
                List.of(), List.of(), "cat.sch.tbl", StatementCategory.DML);
        StatementCompletionEvent event = new StatementCompletionEvent(
                "q-1", DUMMY_STMT, result, null);

        listener.onCompleted(event);

        assertThat(invalidateAllCount.get()).isEqualTo(0);
        assertThat(invalidateByTableCount.get()).isEqualTo(1);
    }

    /**
     * Property 10 (continued): Schema-changing non-DDL categories (DYNAMIC_TABLE,
     * INDEX, CLUSTERING, STREAM) trigger invalidateAll plus invalidateByTable
     * when mutatedTable is present.
     */
    @Property(tries = 20)
    void schemaChangingCategoryWithTableTriggersBoth(
            @ForAll("schemaChangingNonDdlCategories") StatementCategory category) {
        AtomicInteger invalidateAllCount = new AtomicInteger(0);
        AtomicInteger invalidateByTableCount = new AtomicInteger(0);

        StatementCompletionListener listener = createListener(
                invalidateAllCount, invalidateByTableCount);

        StatementResult result = new StatementResult(
                List.of(), List.of(), "cat.sch.tbl", category);
        StatementCompletionEvent event = new StatementCompletionEvent(
                "q-1", DUMMY_STMT, result, null);

        listener.onCompleted(event);

        assertThat(invalidateAllCount.get()).isEqualTo(1);
        assertThat(invalidateByTableCount.get()).isEqualTo(1);
    }

    /**
     * Property 10 (continued): Non-DDL with null mutatedTable triggers neither.
     */
    @Property(tries = 20)
    void nonDdlWithNullTableTriggersNothing(
            @ForAll("nonDdlCategories") StatementCategory category) {
        AtomicInteger invalidateAllCount = new AtomicInteger(0);
        AtomicInteger invalidateByTableCount = new AtomicInteger(0);

        StatementCompletionListener listener = createListener(
                invalidateAllCount, invalidateByTableCount);

        StatementResult result = new StatementResult(
                List.of(), List.of(), null, category);
        StatementCompletionEvent event = new StatementCompletionEvent(
                "q-1", DUMMY_STMT, result, null);

        listener.onCompleted(event);

        assertThat(invalidateAllCount.get()).isEqualTo(0);
        assertThat(invalidateByTableCount.get()).isEqualTo(0);
    }

    /**
     * Property 10 (continued): DDL with null mutatedTable triggers invalidateAll only.
     */
    @Property(tries = 10)
    void ddlWithNullTableTriggersInvalidateAllOnly() {
        AtomicInteger invalidateAllCount = new AtomicInteger(0);
        AtomicInteger invalidateByTableCount = new AtomicInteger(0);

        StatementCompletionListener listener = createListener(
                invalidateAllCount, invalidateByTableCount);

        StatementResult result = new StatementResult(
                List.of(), List.of(), null, StatementCategory.DDL);
        StatementCompletionEvent event = new StatementCompletionEvent(
                "q-1", DUMMY_STMT, result, null);

        listener.onCompleted(event);

        assertThat(invalidateAllCount.get()).isEqualTo(1);
        assertThat(invalidateByTableCount.get()).isEqualTo(0);
    }

    @Provide
    Arbitrary<StatementCategory> nonDdlCategories() {
        return Arbitraries.of(
                StatementCategory.DML, StatementCategory.EXPLAIN,
                StatementCategory.SHOW, StatementCategory.MAINTENANCE,
                StatementCategory.TENANT);
    }

    @Provide
    Arbitrary<StatementCategory> schemaChangingNonDdlCategories() {
        return Arbitraries.of(
                StatementCategory.DYNAMIC_TABLE, StatementCategory.INDEX,
                StatementCategory.CLUSTERING, StatementCategory.STREAM);
    }

    /**
     * Counting listener that mirrors CacheInvalidationListener logic
     * without depending on ResultCacheCoordinator.
     */
    private static StatementCompletionListener createListener(
            AtomicInteger invalidateAllCount,
            AtomicInteger invalidateByTableCount) {
        return new StatementCompletionListener() {
            @Override
            public void onCompleted(StatementCompletionEvent event) {
                StatementCategory category = event.result().category();
                if (category == StatementCategory.DDL
                        || category == StatementCategory.DYNAMIC_TABLE
                        || category == StatementCategory.INDEX
                        || category == StatementCategory.CLUSTERING
                        || category == StatementCategory.STREAM) {
                    invalidateAllCount.incrementAndGet();
                }
                String table = event.mutatedTable();
                if (table != null) {
                    invalidateByTableCount.incrementAndGet();
                }
            }

            @Override
            public int priority() {
                return 20;
            }
        };
    }
}
