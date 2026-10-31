package org.pangrova.coordinator.query.security;

import org.junit.jupiter.api.Test;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.security.api.AccessDeniedException;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL grants and policy binding over a published Iceberg generation. */
class DynamicTableReadAuthorizationTest extends ContainerTestBase {

    private static final QualifiedTableName LOGICAL =
            new QualifiedTableName("default", "default", "rollup");

    @Test
    void logicalGrantAppliesLogicalRowAndColumnPoliciesToPhysicalScan() throws Exception {
        try (var fixture = new SecuritySqlFixture(Files.createTempDirectory("dt-secure-plan"))) {
            fixture.grantTable("analysts", Privilege.SELECT, LOGICAL.table());
            fixture.savePolicy(new RowPolicy(0, "dt-region", LOGICAL.catalog(), LOGICAL.schema(),
                    LOGICAL.table(), "region = 'US'", List.of("analysts"), true, 100, null));
            fixture.saveMask(new ColumnMask(0, "dt-mask", LOGICAL.catalog(), LOGICAL.schema(),
                    LOGICAL.table(), "department", "'hidden'", List.of("analysts"), List.of(), true, null));
            SqlCompiler compiler = fixture.dynamicTableCompiler();
            var session = SecuritySqlFixture.session("alice", Set.of("analysts"));
            var plan = compiler.compileForPolicyApplication(
                    "SELECT id, department FROM default.default.rollup").toPlannedQuery();

            assertThat(plan.physicalToLogicalDynamicTables()).containsEntry(fixture.dynamicPhysical(), LOGICAL);
            var protectedPlan = fixture.applier(compiler).applyPoliciesObserved(plan, session);
            var policy = protectedPlan.policies().get(fixture.dynamicPhysical().toString());
            assertThat(policy.rowFilter().orElseThrow()).contains("region", "US");
            assertThat(policy.columnMasks()).containsEntry("department",
                    java.util.Optional.of("'hidden'"));
            assertThat(protectedPlan.plan().tableReferences()).containsExactly(List.of(
                    fixture.dynamicPhysical().catalog(), fixture.dynamicPhysical().schema(),
                    fixture.dynamicPhysical().table()));
        }
    }

    @Test
    void physicalGrantCannotReplaceLogicalGrantOrPermitDirectGenerationRead() throws Exception {
        try (var fixture = new SecuritySqlFixture(Files.createTempDirectory("dt-denied-plan"))) {
            SqlCompiler compiler = fixture.dynamicTableCompiler();
            fixture.grantTable("no_access", Privilege.SELECT, fixture.dynamicPhysical().table());
            var session = SecuritySqlFixture.session("bob", Set.of("no_access"));

            assertThatThrownBy(() -> fixture.applier(compiler).compileAndApplyPolicies(
                    "SELECT id FROM default.default.rollup", session))
                    .isInstanceOf(AccessDeniedException.class);
            assertThatThrownBy(() -> fixture.applier(compiler).compileAndApplyPolicies(
                    "SELECT id FROM " + fixture.dynamicPhysical(), session))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("Direct access");
        }
    }

    @Test
    void quotedOrAmbiguousNamesNeverInheritAnotherDynamicTablePublication() throws Exception {
        try (var fixture = new SecuritySqlFixture(Files.createTempDirectory("dt-name-plan"))) {
            fixture.grantTable("analysts", Privilege.SELECT, LOGICAL.table());
            SqlCompiler compiler = fixture.dynamicTableCompiler();
            var session = SecuritySqlFixture.session("alice", Set.of("analysts"));

            assertThatThrownBy(() -> fixture.applier(compiler).compileAndApplyPolicies(
                    "SELECT id FROM default.other.rollup", session)).isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> fixture.applier(compiler).compileAndApplyPolicies(
                    "SELECT id FROM default.default.\"Rollup\"", session))
                    .isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> fixture.applier(compiler).compileAndApplyPolicies(
                    "SELECT id FROM \"default.default\".rollup", session))
                    .isInstanceOf(RuntimeException.class);
        }
    }
}
