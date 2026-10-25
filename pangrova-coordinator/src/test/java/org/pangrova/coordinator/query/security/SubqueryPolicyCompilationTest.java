package org.pangrova.coordinator.query.security;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.testing.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Actual catalog binding, PostgreSQL policy application, optimization and wire conversion. */
class SubqueryPolicyCompilationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM source_orders s WHERE s.id = o.id)",
            "SELECT o.id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM source_orders s WHERE s.id = o.id)",
            "SELECT (SELECT COUNT(*) FROM source_orders s WHERE s.id = o.id) AS n FROM orders o"
    })
    void correlatedMembershipKeepsInnerRowPolicyThroughWireConversion(String sql) throws Exception {
        try (var fixture = new SecuritySqlFixture(warehouse)) {
            fixture.authorizePolicyReaders();
            fixture.savePolicy(new RowPolicy(0, "first_row", "default", "default", "source_orders",
                    "id = 1", List.of("sales_team"), true, 1));
            var actor = SecuritySqlFixture.session("sales_user", Set.of("sales_team"));
            var plan = fixture.applier().compileAndApplyPolicies(sql, actor);
            assertThat(plan.tableReferences()).contains(List.of("default", "default", "orders"),
                    List.of("default", "default", "source_orders"));
            assertThatCode(() -> fixture.compiler.convertValidateAndSerialize(
                    plan.plan(), plan.queryOutputColumns()))
                    .as("Protected optimized relation:%n%s", plan.planExplanation()).doesNotThrowAnyException();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM source_orders s WHERE s.id + 1 = o.id)",
            "SELECT o.id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM source_orders s WHERE s.id + 1 = o.id)",
            "SELECT (SELECT COUNT(*) FROM source_orders s WHERE s.id + 1 = o.id) AS n FROM orders o",
            "SELECT (SELECT SUM(id) FROM source_orders s WHERE s.id + 1 = o.id) AS n FROM orders o"
    })
    void nullMaskRemovesTheActualCorrelationBeforeWireConversion(String sql) throws Exception {
        try (var fixture = new SecuritySqlFixture(warehouse)) {
            fixture.authorizePolicyReaders();
            fixture.saveMask(new ColumnMask(0, "null_key", "default", "default", "source_orders", "id",
                    "CAST(NULL AS BIGINT)", List.of("sales_team"), List.of(), true, null));
            var actor = SecuritySqlFixture.session("sales_user", Set.of("sales_team"));
            var plan = fixture.applier().compileAndApplyPolicies(sql, actor);
            assertThatCode(() -> fixture.compiler.convertValidateAndSerialize(
                    plan.plan(), plan.queryOutputColumns()))
                    .as("Protected optimized relation:%n%s", plan.planExplanation()).doesNotThrowAnyException();
        }
    }
}
