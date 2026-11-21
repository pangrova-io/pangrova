package org.pangrova.planner.analysis;

import org.pangrova.planner.api.exception.PlannerException;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.type.PlannerTypeFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Native-plan input must contain expanded base relations with the view's own name scope. */
class PlannerViewExpansionTest {
    private final CalciteSchema root = CalciteSchema.createRootSchema(false);
    private final org.apache.calcite.schema.SchemaPlus schema = root.plus().add("home", new AbstractSchema());
    private final RelDataType rowType = new PlannerTypeFactory().builder().add("id", SqlTypeName.BIGINT).build();

    private void table(String name) {
        schema.add(name, new AbstractTable() {
            @Override
            public RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory factory) {
                return factory.copyType(rowType);
            }
        });
    }

    private void view(String name, String sql) {
        schema.add(name, new ViewTable(Object[].class, RelDataTypeImpl.proto(rowType), sql,
                List.of("home"), List.of("home", name)));
    }

    @Test
    void unqualifiedViewBodyUsesDefinitionScopeAndPreservesBaseTableIdentity() {
        table("orders");
        root.plus().add("caller", new AbstractSchema()).add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.VARCHAR).build();
            }
        });
        view("order_view", "SELECT id FROM orders");
        var plan = new PangrovaPlanner(root, List.of("caller")).plan("SELECT id FROM home.order_view").plan();
        assertThat(scans(plan)).containsExactly(List.of("home", "orders"));
        assertThat(plan.getRowType().getFieldList().get(0).getType().getSqlTypeName()).isEqualTo(SqlTypeName.BIGINT);
    }

    @Test
    void nestedViewsCtesAliasesAndRepeatedReferencesExpandWithoutRetainingViewScans() {
        table("orders");
        view("inner_view", "SELECT id FROM orders WHERE id > 0");
        view("outer_view", "WITH chosen AS (SELECT id FROM inner_view) SELECT id FROM chosen");
        var plan = new PangrovaPlanner(root, List.of("home")).plan(
                "SELECT a.id FROM outer_view a JOIN outer_view b ON a.id=b.id").plan();
        assertThat(scans(plan)).hasSize(2).containsOnly(List.of("home", "orders"));
    }

    @Test
    void viewNameInsideStringLiteralRemainsData() {
        table("orders");
        view("order_view", "SELECT id FROM orders");
        var plan = new PangrovaPlanner(root, List.of("home")).plan(
                "SELECT id, 'home.order_view' AS marker FROM home.order_view").plan();
        assertThat(scans(plan)).containsExactly(List.of("home", "orders"));
        assertThat(org.apache.calcite.plan.RelOptUtil.toString(plan)).contains("'home.order_view'");
    }

    @Test
    void directAndIndirectCyclesFailBeforeOptimization() {
        view("self", "SELECT id FROM self");
        view("first_view", "SELECT id FROM second_view");
        view("second_view", "SELECT id FROM first_view");
        var planner = new PangrovaPlanner(root, List.of("home"));
        assertThatThrownBy(() -> planner.plan("SELECT id FROM self"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("Recursive or excessively nested view");
        assertThatThrownBy(() -> planner.plan("SELECT id FROM first_view"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("Recursive or excessively nested view");
        table("orders");
        view("valid_view", "SELECT id FROM orders");
        assertThat(scans(planner.plan("SELECT id FROM valid_view").plan()))
                .containsExactly(List.of("home", "orders"));
    }

    @Test
    void viewDefinitionCannotIntroduceMutation() {
        table("orders");
        view("mutating_view", "INSERT INTO orders VALUES (1)");
        assertThatThrownBy(() -> new PangrovaPlanner(root, List.of("home")).plan("SELECT id FROM mutating_view"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("read-only query");
    }

    @Test
    void excessiveAcyclicViewDepthFailsAtTheExpansionBound() {
        table("orders");
        for (int depth = 64; depth >= 0; depth--) {
            view("nested_" + depth, "SELECT id FROM " + (depth == 64 ? "orders" : "nested_" + (depth + 1)));
        }
        assertThatThrownBy(() -> new PangrovaPlanner(root, List.of("home")).plan("SELECT id FROM nested_0"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("Recursive or excessively nested view");
    }

    @Test
    void missingRelationFailsWithoutAReplacementPlan() {
        view("invalid_view", "SELECT id FROM missing_table");
        assertThatThrownBy(() -> new PangrovaPlanner(root, List.of("home")).plan("SELECT id FROM invalid_view"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("missing_table");
    }

    private static List<List<String>> scans(RelNode root) {
        var names = new ArrayList<List<String>>();
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan scan) names.add(scan.getTable().getQualifiedName());
                super.visit(node, ordinal, parent);
            }
        }.go(root);
        return names;
    }
}
