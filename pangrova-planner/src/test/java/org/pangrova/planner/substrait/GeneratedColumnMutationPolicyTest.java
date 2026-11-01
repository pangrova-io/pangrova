package org.pangrova.planner.substrait;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexInterpreter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.embedding.GeneratedMutationFixture;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.security.core.authz.DynamicRbacAccessControl;
import org.pangrova.security.core.authz.InMemoryRoleStore;
import org.pangrova.spi.security.policy.Role;
import org.pangrova.security.core.authz.StaticGrantStore;
import org.pangrova.spi.security.policy.UserRoleAssignment;
import org.pangrova.spi.security.mask.ColumnMask;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.security.core.mask.provider.memory.InMemoryEnrichedColumnMaskStore;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.rowfilter.RowPolicy;
import org.pangrova.persistence.api.security.rowfilter.RowPolicyStore;
import org.pangrova.spi.security.UserIdentity;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual policy appliers and the complete replacement relation consumed by Native DML. */
class GeneratedColumnMutationPolicyTest {
    private static final UserIdentity WRITER = new UserIdentity("writer", Set.of("idp_group"), Map.of(), Instant.EPOCH);

    @Test
    void unchangedSourceIsMaskedInsideTheNewEmbeddingButOriginalVectorSurvivesOutsideMatches() {
        var modify = protectedPlan("UPDATE documents SET title = 'new-title' WHERE id > 1");
        var assignments = (LogicalProject) modify.getInput();
        var embedding = (RexCall) assignments.getProjects().get(6);
        var source = (RexCall) embedding.getOperands().get(1);
        assertThat(GeneratedMutationFixture.textLiteral(source.getOperands().get(0))).isEqualTo("new-title");
        assertThat(GeneratedMutationFixture.textLiteral(source.getOperands().get(2))).isEqualTo("masked-body");

        var replacement = (LogicalProject) lower(modify);
        assertThat(replacement.getInput()).isInstanceOf(TableScan.class);
        var vector = (RexCall) replacement.getProjects().get(4);
        assertThat(vector.getKind()).isEqualTo(SqlKind.CASE);
        assertThat(((RexCall) vector.getOperands().get(1)).getOperator().getName()).isEqualTo("AI_EMBED");
        assertThat(((RexInputRef) vector.getOperands().get(2)).getIndex()).isEqualTo(4);
        assertThat(match(vector, replacement.getInput(), 3L, 1L)).isEqualTo(true);
        assertThat(match(vector, replacement.getInput(), 3L, 2L)).isEqualTo(false);
        assertThat(match(vector, replacement.getInput(), 1L, 1L)).isEqualTo(false);
        assertThat(match(vector, replacement.getInput(), null, 1L)).isNotEqualTo(true);
    }

    @Test
    void sourceSelfAssignmentStillReadsTheMaskedNewValueNotTheRawPhysicalSurvivor() {
        var modify = protectedPlan("UPDATE documents SET title = body WHERE id > 1");
        var assignments = (LogicalProject) modify.getInput();
        var source = (RexCall) ((RexCall) assignments.getProjects().get(6)).getOperands().get(1);
        assertThat(GeneratedMutationFixture.textLiteral(source.getOperands().get(0))).isEqualTo("masked-body");
        assertThat(GeneratedMutationFixture.textLiteral(source.getOperands().get(2))).isEqualTo("masked-body");
        var replacement = (LogicalProject) lower(modify);
        assertThat(((RexInputRef) replacement.getProjects().get(3)).getIndex()).isEqualTo(3);
    }

    @Test
    void mergeProtectsBothActionsWithoutMaskingOldPhysicalTargetVectors() {
        var modify = protectedPlan("MERGE INTO documents t USING source_rows s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET title = s.title "
                + "WHEN NOT MATCHED THEN INSERT (id, tenant, title, body) VALUES (s.id, s.tenant, s.title, s.body)");
        var converted = new SubstraitConverter().convertWithResult(modify, List.of());
        assertThat(converted.aiInvocationHints()).hasSize(2);
        var replacement = (LogicalUnion) lower(modify);
        var updated = (LogicalProject) replacement.getInputs().get(0);
        var vector = (RexCall) updated.getProjects().get(4);
        assertThat(vector.getKind()).isEqualTo(SqlKind.CASE);
        assertThat(((RexCall) vector.getOperands().get(1)).getOperator().getName()).isEqualTo("AI_EMBED");
        assertThat(((RexInputRef) vector.getOperands().get(2)).getIndex()).isEqualTo(4);
        var inserted = (LogicalProject) replacement.getInputs().get(1);
        var insertedVector = (RexCall) inserted.getProjects().get(4);
        assertThat(insertedVector.getKind()).isEqualTo(SqlKind.CAST);
        assertThat(((RexCall) insertedVector.getOperands().get(0)).getOperator().getName()).isEqualTo("AI_EMBED");
        assertThat(inserted.getInput().explain()).contains("masked-body");
        assertThat(new SubstraitPlanSerializer().serialize(converted.plan())).isNotEmpty();
    }

    private static LogicalTableModify protectedPlan(String sql) {
        RelNode modify = GeneratedMutationFixture.plan(sql);
        var roles = new InMemoryRoleStore();
        roles.createRole(Role.of("writers"));
        roles.grantRoleToUser(UserRoleAssignment.of("writer", "writers"));
        var masks = new InMemoryEnrichedColumnMaskStore();
        for (String table : List.of("documents", "source_rows")) {
            masks.saveColumnMask(new ColumnMask(table.equals("documents") ? 1 : 2,
                    "mask_body", "catalog", "schema", table, "body", "'masked-body'",
                    List.of("writers"), List.of(), true, null));
        }
        var policy = new RowPolicy(1, "own_tenant", "catalog", "schema", "documents",
                "tenant = 1", List.of("writers"), true, 1, null);
        var authority = new DynamicRbacAccessControl(new StaticGrantStore(List.of()), roles,
                new RowLevelSecurityFilter(new ImmutablePolicies(policy)), new ColumnMaskingProcessor(masks));
        modify = new ColumnMaskApplier(authority).applyColumnMasks(modify, WRITER);
        return (LogicalTableModify) new RowFilterApplier(authority).applyRowFilters(modify, WRITER);
    }

    private static RelNode lower(LogicalTableModify modify) {
        AtomicReference<RelNode> result = new AtomicReference<>();
        new DmlOperationConverter(node -> {
            result.set(node);
            return new SubstraitConverter().convert(node).getRoots().get(0).getInput();
        }, join -> { }).convert(modify);
        return result.get();
    }

    @SuppressWarnings("rawtypes")
    private static Comparable match(RexCall vectorCase, RelNode input, Long id, Long tenant) {
        Map<RexNode, Comparable> values = new HashMap<>();
        values.put(new RexInputRef(0, input.getRowType().getFieldList().get(0).getType()),
                id == null ? org.apache.calcite.rel.metadata.NullSentinel.INSTANCE : id);
        values.put(new RexInputRef(1, input.getRowType().getFieldList().get(1).getType()),
                tenant == null ? org.apache.calcite.rel.metadata.NullSentinel.INSTANCE : tenant);
        return RexInterpreter.evaluate(vectorCase.getOperands().get(0), values);
    }

    private record ImmutablePolicies(RowPolicy policy) implements RowPolicyStore {
        @Override public List<RowPolicy> getPolicies(String catalog, String schema, String table) {
            return table.equals("documents") && catalog.equals("catalog") && schema.equals("schema")
                    ? List.of(policy) : List.of();
        }
        @Override public void savePolicy(RowPolicy value) { throw new UnsupportedOperationException("immutable fixture"); }
        @Override public void deletePolicy(long id) { throw new UnsupportedOperationException("immutable fixture"); }
    }
}
