package org.pangrova.coordinator.query.security;

import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.TableScanColumnUsage;
import org.pangrova.planner.security.expression.MaskExpressionParser;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.security.authz.DefaultAccessControl;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.StaticGrantStore;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.EnrichedColumnMaskStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.pangrova.coordinator.testinfra.CoordinatorRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ColumnMaskApplier column masking integration.
 * Validates: Requirements 4.2, 4.5.
 */
class ColumnMaskApplierTest {

    private static final SqlCompiler COMPILER = buildCompilerWithOrdersTable();

    @Test
    void nullIdentitySkipsMasking() {
        ColumnMaskApplier applier = new ColumnMaskApplier(buildAccessControlWithMasks(List.of()));
        RelNode relNode = COMPILER.compileToRelNode("SELECT 1");

        RelNode result = applier.applyColumnMasks(relNode, null);

        assertThat(result).isSameAs(relNode);
    }

    @Test
    void constantQueryNoTableScanReturnsSameNode() {
        ColumnMaskApplier applier = new ColumnMaskApplier(buildAccessControlWithMasks(List.of()));
        RelNode relNode = COMPILER.compileToRelNode("SELECT 1 + 2");

        RelNode result = applier.applyColumnMasks(relNode, testIdentity("user1", Set.of("readers")));

        assertThat(result).isSameAs(relNode);
    }

    @Test
    void emptyMaskStoreReturnsSameNode() {
        ColumnMaskApplier applier = new ColumnMaskApplier(buildAccessControlWithMasks(List.of()));
        RelNode relNode = COMPILER.compileToRelNode("SELECT 1");

        RelNode result = applier.applyColumnMasks(relNode, testIdentity("user1", Set.of("readers")));

        assertThat(result).isSameAs(relNode);
    }

    @Test
    void varcharColumnMaskApplied() {
        ColumnMask mask = new ColumnMask(
                1L, "mask_dept", "default", "default", "orders",
                "department", "'***'", List.of("analysts"), List.of(), true, null);
        ColumnMaskApplier applier = new ColumnMaskApplier(buildAccessControlWithMasks(List.of(mask)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyColumnMasks(relNode, testIdentity("user1", Set.of("analysts")));

        // The result should differ from input (mask was applied)
        assertThat(result.explain()).isNotEqualTo(relNode.explain());
        // Find the mask-injected project
        LogicalProject project = findMaskProject(result);
        assertThat(project).isNotNull();
        // department is at index 1; the masked expression should be a CAST or literal
        RexNode maskedExpr = project.getProjects().get(1);
        // The type should be compatible with VARCHAR(100)
        assertThat(maskedExpr.getType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    }

    @Test
    void numericColumnMaskProducesCorrectType() {
        ColumnMask mask = new ColumnMask(
                2L, "mask_amount", "default", "default", "orders",
                "amount", "0", List.of("analysts"), List.of(), true, null);
        ColumnMaskApplier applier = new ColumnMaskApplier(buildAccessControlWithMasks(List.of(mask)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyColumnMasks(relNode, testIdentity("user1", Set.of("analysts")));

        assertThat(result.explain()).isNotEqualTo(relNode.explain());
        LogicalProject project = findMaskProject(result);
        assertThat(project).isNotNull();
        // amount is at index 4; the masked expression should be cast to DECIMAL
        RexNode maskedExpr = project.getProjects().get(4);
        assertThat(maskedExpr.getType().getSqlTypeName()).isEqualTo(SqlTypeName.DECIMAL);
    }

    @Test
    void bigintColumnMaskWithZeroProducesIntegerType() {
        ColumnMask mask = new ColumnMask(
                3L, "mask_id", "default", "default", "orders",
                "id", "0", List.of("analysts"), List.of(), true, null);
        ColumnMaskApplier applier = new ColumnMaskApplier(buildAccessControlWithMasks(List.of(mask)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyColumnMasks(relNode, testIdentity("user1", Set.of("analysts")));

        assertThat(result.explain()).isNotEqualTo(relNode.explain());
        LogicalProject project = findMaskProject(result);
        assertThat(project).isNotNull();
        // id is at index 0; the masked expression should be cast to BIGINT
        RexNode maskedExpr = project.getProjects().get(0);
        assertThat(maskedExpr.getType().getSqlTypeName()).isEqualTo(SqlTypeName.BIGINT);
    }

    @Test
    void castExpressionMaskApplied() {
        ColumnMask mask = new ColumnMask(
                4L, "mask_amount_zero", "default", "default", "orders",
                "amount", "CAST(0 AS DECIMAL(10,2))", List.of("analysts"), List.of(), true, null);
        ColumnMaskApplier applier = new ColumnMaskApplier(buildAccessControlWithMasks(List.of(mask)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyColumnMasks(relNode, testIdentity("user1", Set.of("analysts")));

        assertThat(result.explain()).isNotEqualTo(relNode.explain());
        LogicalProject project = findMaskProject(result);
        assertThat(project).isNotNull();
        RexNode maskedExpr = project.getProjects().get(4);
        assertThat(maskedExpr.getType().getSqlTypeName()).isEqualTo(SqlTypeName.DECIMAL);
    }

    @Test
    void maskNotAppliedWhenUserNotInRole() {
        ColumnMask mask = new ColumnMask(
                5L, "mask_dept", "default", "default", "orders",
                "department", "'***'", List.of("analysts"), List.of(), true, null);
        ColumnMaskApplier applier = new ColumnMaskApplier(buildAccessControlWithMasks(List.of(mask)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyColumnMasks(relNode, testIdentity("user1", Set.of("other_team")));

        // No mask applied, result should be structurally identical to input
        assertThat(result.explain()).isEqualTo(relNode.explain());
    }

    @Test
    void exemptUserSeesRawData() {
        ColumnMask mask = new ColumnMask(
                6L, "mask_dept", "default", "default", "orders",
                "department", "'***'", List.of("analysts"), List.of("admins"), true, null);
        ColumnMaskApplier applier = new ColumnMaskApplier(buildAccessControlWithMasks(List.of(mask)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyColumnMasks(relNode, testIdentity("admin1", Set.of("analysts", "admins")));

        // Exempt user should not have mask applied, result structurally identical
        assertThat(result.explain()).isEqualTo(relNode.explain());
    }

    @Test
    void effectivePlanDigestChangesWhenColumnMaskDefinitionChanges() {
        ColumnMask redacted = new ColumnMask(
                7L, "mask_dept", "default", "default", "orders",
                "department", "'***'", List.of("analysts"), List.of(), true, null);
        ColumnMask hidden = new ColumnMask(
                8L, "mask_dept", "default", "default", "orders",
                "department", "'[hidden]'", List.of("analysts"), List.of(), true, null);
        UserIdentity identity = testIdentity("user1", Set.of("analysts"));
        var base = COMPILER.compileToPlannedQuery("SELECT * FROM orders");

        String redactedDigest = base.applyColumnMasks(
                new ColumnMaskApplier(buildAccessControlWithMasks(List.of(redacted))),
                identity).effectivePlanDigest();
        String hiddenDigest = base.applyColumnMasks(
                new ColumnMaskApplier(buildAccessControlWithMasks(List.of(hidden))),
                identity).effectivePlanDigest();

        assertThat(redactedDigest).isNotEqualTo(hiddenDigest);
    }

    /**
     * Finds a LogicalProject that was injected by the mask applier.
     * The mask project wraps a TableScan directly (its input is a TableScan).
     */
    private static LogicalProject findMaskProject(RelNode node) {
        if (node instanceof LogicalProject project) {
            for (RelNode input : project.getInputs()) {
                if (input instanceof org.apache.calcite.rel.core.TableScan) {
                    return project;
                }
            }
        }
        for (RelNode input : node.getInputs()) {
            LogicalProject found = findMaskProject(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * Builds a SqlCompiler with a schema containing an "orders" table
     * with columns of various types to test mask type correctness.
     */
    private static SqlCompiler buildCompilerWithOrdersTable() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("id", SqlTypeName.BIGINT)
                        .add("department", SqlTypeName.VARCHAR, 100)
                        .add("region", SqlTypeName.VARCHAR, 50)
                        .add("status", SqlTypeName.VARCHAR, 50)
                        .add("amount", SqlTypeName.DECIMAL, 10, 2)
                        .build();
            }
        });
        PangrovaPlanner planner = new PangrovaPlanner(rootSchema);
        return new SqlCompiler(planner,
                new org.pangrova.planner.substrait.SubstraitConverter(),
                new org.pangrova.planner.substrait.SubstraitPlanSerializer());
    }

    private static AccessControl buildAccessControlWithMasks(List<ColumnMask> masks) {
        EnrichedColumnMaskStore maskStore = new EnrichedColumnMaskStore() {
            @Override
            public List<ColumnMask> getTableMasks(String catalog, String schema, String table) {
                return masks.stream().filter(mask -> mask.catalogName().equals(catalog)
                        && mask.schemaName().equals(schema) && mask.tableName().equals(table)).toList();
            }

            @Override
            public List<ColumnMask> getColumnMasks(String c, String s, String t, String col) {
                return masks.stream()
                        .filter(m -> m.catalogName().equals(c) && m.schemaName().equals(s)
                                && m.tableName().equals(t) && m.columnName().equals(col))
                        .toList();
            }
            @Override public void saveColumnMask(ColumnMask m) { }
            @Override public void deleteMask(long id) { }
            @Override
            public List<org.pangrova.spi.security.ColumnMaskRecord> getMasks(String c, String s, String t, String col) {
                return getColumnMasks(c, s, t, col).stream().map(ColumnMask::toRecord).toList();
            }
            @Override public void saveMask(org.pangrova.spi.security.ColumnMaskRecord m) { }
        };
        return new DynamicRbacAccessControl(
                new StaticGrantStore(List.of()),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("readers", "readers"), java.util.Map.entry("analysts", "analysts"), java.util.Map.entry("other_team", "other_team"), java.util.Map.entry("admins", "admins"))),
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(maskStore));
    }

    private static UserIdentity testIdentity(String principal, Set<String> groups) {
        return new UserIdentity(principal, groups, Map.of(), Instant.now());
    }
}
