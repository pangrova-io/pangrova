package org.pangrova.governance.quality.store;

import org.junit.jupiter.api.Test;
import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.Severity;
import org.pangrova.governance.testinfra.SharedPostgresContainer;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Real result history across new executions and concurrent writers. */
class JdbcQualityResultIdentityIntegrationTest extends ContainerTestBase {
    @Test
    void zeroSentinelMeansANewExecutionAcrossStoreReopen() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("quality_result_identity");
        new JdbcQualityRuleStore(dataSource).saveResult(result("before"));
        new JdbcQualityRuleStore(dataSource).saveResult(result("after"));
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT result_id,rule_name FROM quality_check_results ORDER BY result_id")) {
            var names = new ArrayList<String>();
            while (rows.next()) {
                assertThat(rows.getLong(1)).isPositive();
                names.add(rows.getString(2));
            }
            assertThat(names).containsExactly("before", "after");
        }
    }

    @Test
    void concurrentNewExecutionsNeverOverwriteEachOther() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("quality_result_concurrent");
        var executor = Executors.newFixedThreadPool(4);
        var start = new CountDownLatch(1);
        try {
            var pending = new ArrayList<java.util.concurrent.Future<?>>();
            for (int writer = 0; writer < 4; writer++) {
                int id = writer;
                pending.add(executor.submit(() -> {
                    if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("start missing");
                    var store = new JdbcQualityRuleStore(dataSource);
                    for (int run = 0; run < 10; run++) store.saveResult(result("writer-" + id + "-run-" + run));
                    return null;
                }));
            }
            start.countDown();
            for (var future : pending) future.get(20, TimeUnit.SECONDS);
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                    var rows = statement.executeQuery("SELECT count(*),count(DISTINCT result_id),count(DISTINCT rule_name),"
                            + "min(result_id) FROM quality_check_results")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(40);
                assertThat(rows.getInt(2)).isEqualTo(40);
                assertThat(rows.getInt(3)).isEqualTo(40);
                assertThat(rows.getLong(4)).isPositive();
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void actualWriteGatePreservesBothConfigurationFailureResults() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("quality_result_gate");
        var store = new JdbcQualityRuleStore(dataSource);
        var time = Instant.parse("2026-01-01T00:00:00Z");
        store.saveRule(new org.pangrova.governance.quality.rule.DataQualityRule(1, "invalid-config",
                "catalog", "schema", "orders", "", Severity.ERROR, "invalid", true, null, time, time,
                new org.pangrova.governance.quality.rule.QualityExecutionIdentity("quality-owner", "quality-tenant"), null));
        var gate = new org.pangrova.governance.quality.gate.RuleBasedWriteQualityGate(store,
                new org.pangrova.governance.quality.rule.UnconfiguredQualityRuleExecutor());
        assertThat(gate.validate("catalog", "schema", "orders").allowed()).isFalse();
        assertThat(gate.validate("catalog", "schema", "orders").allowed()).isFalse();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT count(*),count(DISTINCT result_id),bool_and(NOT passed) "
                        + "FROM quality_check_results WHERE rule_id=1 AND rule_name='invalid-config'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(2);
            assertThat(rows.getInt(2)).isEqualTo(2);
            assertThat(rows.getBoolean(3)).isTrue();
        }
    }

    private static QualityCheckResult result(String name) {
        return new QualityCheckResult(0, 1, name, "orders", Severity.ERROR,
                Instant.parse("2026-01-01T00:00:00Z"), false, 10, 2, 1, "actual failure", null);
    }
}
