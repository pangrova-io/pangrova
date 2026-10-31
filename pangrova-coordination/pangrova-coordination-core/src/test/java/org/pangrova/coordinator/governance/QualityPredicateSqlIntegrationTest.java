package org.pangrova.coordinator.governance;

import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.GovernanceQueryException;
import org.pangrova.governance.api.quality.QualityExecutionIdentity;
import org.pangrova.governance.api.quality.QualityQueryResult;
import org.pangrova.governance.quality.executor.sql.SqlQualityRuleExecutor;

import org.junit.jupiter.api.Test;
import org.pangrova.governance.api.quality.Severity;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Executes generated quality SQL against PostgreSQL; not Native or public SQL E2E. */
class QualityPredicateSqlIntegrationTest extends ContainerTestBase {

    @Test
    void falseAndUnknownPredicatesBothViolateTheRuleInActualSql() throws Exception {
        var postgres = QualityPostgres.POSTGRES;
        String schema = "quality_predicate_" + UUID.randomUUID().toString().replace("-", "");
        try (var connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
            statement.execute("CREATE TABLE " + schema + ".rows (id BIGINT, amount BIGINT)");
            statement.execute("INSERT INTO " + schema + ".rows VALUES (1, 1), (2, -1), (3, 0), (4, NULL)");
            var samples = new ArrayList<List<String>>();
            var executor = new SqlQualityRuleExecutor((request, identity) -> executeSql(connection, QualityReadSql.render(request), samples));
            var now = Instant.now();
            var rule = new DataQualityRule(1, "positive_amount", connection.getCatalog(), schema,
                    "rows", "amount > 0", Severity.WARNING, "must be positive", true, null, now, now,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null);

            var result = executor.execute(rule);

            assertThat(result.passed()).isFalse();
            assertThat(result.totalRows()).isEqualTo(4);
            assertThat(result.failedRows()).isEqualTo(3);
            assertThat(samples).containsExactlyInAnyOrder(
                    List.of("2", "-1"), List.of("3", "0"), List.of("4", "null"));
            assertThat(result.errorMessage()).contains("3 of 4");
        }
    }

    private static QualityQueryResult executeSql(Connection connection, String sql,
                                                List<List<String>> samples) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            if (sql.startsWith("SELECT COUNT(*)")) {
                assertThat(result.next()).isTrue();
                long total = result.getLong("total_rows");
                long failed = result.getLong("failed_rows");
                assertThat(result.next()).isFalse();
                return new QualityQueryResult(total, failed, List.of());
            }
            while (result.next()) {
                samples.add(List.of(String.valueOf(result.getObject(1)), String.valueOf(result.getObject(2))));
            }
            return new QualityQueryResult(0, 0, samples);
        } catch (SQLException failure) {
            throw new GovernanceQueryException(sql, "Actual PostgreSQL quality query failed", failure);
        }
    }
}
