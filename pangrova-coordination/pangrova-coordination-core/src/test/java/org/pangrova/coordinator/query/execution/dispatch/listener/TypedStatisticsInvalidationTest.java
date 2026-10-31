package org.pangrova.coordinator.query.execution.dispatch.listener;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementCompletionEvent;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.SqlParser;
import org.pangrova.planner.stats.PlannerTableStatistics;
import org.pangrova.planner.stats.StatisticsCollector;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.security.api.UserIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual parser/statement-listener identity semantics, not a successful SQL execution. */
class TypedStatisticsInvalidationTest {
    @Test
    void invalidationUsesAstComponentsAndNotAmbiguousResultDisplay() {
        var first = new QualifiedTableName("bound", "a.b", "rows");
        var second = new QualifiedTableName("bound", "a", "b.rows");
        var collector = new StatisticsCollector();
        collector.putStatistics(first, new PlannerTableStatistics(first, Map.of(), Instant.EPOCH));
        collector.putStatistics(second, new PlannerTableStatistics(second, Map.of(), Instant.EPOCH));
        var event = new StatementCompletionEvent("identity-query",
                new SqlParser().parse("INSERT INTO bound.\"a.b\".rows VALUES (1)"),
                new StatementResult(List.of(), List.of(), second.toString(), StatementCategory.DML), session());
        assertThat(first.toString()).isEqualTo(second.toString());
        assertThat(event.mutatedTableIdentity()).isEqualTo(first);
        new StatisticsInvalidationListener(collector).onCompleted(event);
        assertThat(collector.getStatistics(first)).isEmpty();
        assertThat(collector.getStatistics(second).orElseThrow().tableName()).isEqualTo(second);
    }

    @Test
    void unqualifiedStatementUsesItsTrustedSessionAndRetainsQuotedSpelling() {
        var event = new StatementCompletionEvent("session-query",
                new SqlParser().parse("INSERT INTO \"Camel.Rows\" VALUES (1)"),
                new StatementResult(List.of(), List.of(), "ignored", StatementCategory.DML), session());
        assertThat(event.mutatedTableIdentity()).isEqualTo(new QualifiedTableName("bound", "a.b", "Camel.Rows"));
    }

    private static CoordinatorSessionContext session() {
        return new CoordinatorSessionContext("identity-query", Map.of("catalog", "bound", "schema", "a.b"),
                new UserIdentity("reader", Set.of(), Map.of(), Instant.EPOCH), "tenant");
    }
}
