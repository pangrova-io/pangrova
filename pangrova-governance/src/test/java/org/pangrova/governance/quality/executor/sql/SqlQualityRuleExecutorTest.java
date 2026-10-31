package org.pangrova.governance.quality.executor.sql;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.api.quality.Severity;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.GovernanceQueryException;
import org.pangrova.governance.api.quality.GovernanceQueryRunner;
import org.pangrova.governance.api.quality.QualityExecutionIdentity;
import org.pangrova.governance.api.quality.QualityQueryResult;
import org.pangrova.governance.api.quality.QualityReadRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The governance layer preserves typed read identity and owns the count/sample execution policy. */
class SqlQualityRuleExecutorTest {
    @Test
    void carriesIdentifiersAndPredicateAsDataAndSamplesOnlyViolations() {
        var calls = new ArrayList<QualityReadRequest>();
        var rule = rule("label = 'drop; -- union'", true);
        var executor = new SqlQualityRuleExecutor((request, subject) -> {
            assertThat(subject).isEqualTo(rule.executionIdentity());
            calls.add(request);
            return request.type() == GovernanceQueryRunner.QueryType.COUNTS
                    ? new QualityQueryResult(4, 2, List.of())
                    : new QualityQueryResult(2, 2, List.of(List.of("bad-one"), List.of("bad-two")));
        });
        var result = executor.execute(rule);
        assertThat(result.tableId()).isEqualTo(rule("unused", true).tableId());
        assertThat(result.passed()).isFalse();
        assertThat(result.totalRows()).isEqualTo(4);
        assertThat(result.failedRows()).isEqualTo(2);
        assertThat(result.errorMessage()).contains("Sample violations: 2 rows");
        assertThat(calls).hasSize(2).allSatisfy(request -> {
            assertThat(request.catalog()).isEqualTo("catalog with spaces");
            assertThat(request.schema()).isEqualTo("schema");
            assertThat(request.table()).isEqualTo("quoted\"table");
            assertThat(request.expression()).isEqualTo(rule.checkExpression());
        });
        assertThat(calls).extracting(QualityReadRequest::type).containsExactly(
                GovernanceQueryRunner.QueryType.COUNTS, GovernanceQueryRunner.QueryType.SAMPLES);
    }

    @Test
    void passingCountsDoNotIssueASampleRead() {
        var calls = new ArrayList<QualityReadRequest>();
        var executor = new SqlQualityRuleExecutor((request, subject) -> {
            calls.add(request);
            return new QualityQueryResult(12, 0, List.of());
        });
        assertThat(executor.execute(rule("amount > 0", true)).passed()).isTrue();
        assertThat(calls).hasSize(1);
    }

    @Test
    void disabledAndBlankRulesNeverReachQueryAdmission() {
        var executor = new SqlQualityRuleExecutor((request, subject) -> {
            throw new AssertionError("Invalid rule reached query admission");
        });
        assertThatThrownBy(() -> executor.execute(rule("col > 0", false)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> executor.execute(rule(" ", true)))
                .isInstanceOf(GovernanceQueryException.class);
    }

    @Test
    void AdapterValidationFailureBecomesFailedCheckWithoutFabricatedCounts() {
        var executor = new SqlQualityRuleExecutor((request, subject) -> {
            throw new GovernanceQueryException(request.expression(), "invalid scalar expression");
        });
        var result = executor.execute(rule("col IN (SELECT col FROM other)", true));
        assertThat(result.tableId()).isEqualTo(rule("unused", true).tableId());
        assertThat(result.passed()).isFalse();
        assertThat(result.totalRows()).isZero();
        assertThat(result.failedRows()).isZero();
        assertThat(result.errorMessage()).isEqualTo("invalid scalar expression");
    }

    private static DataQualityRule rule(String expression, boolean enabled) {
        return new DataQualityRule(1, "rule", "catalog with spaces", "schema", "quoted\"table", expression,
                Severity.ERROR, "message", enabled, null, Instant.now(), Instant.now(),
                new QualityExecutionIdentity("owner", "tenant"),
                java.util.UUID.fromString("00000000-0000-0000-0000-000000000001"));
    }
}
