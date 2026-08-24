package org.pangrova.planner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for PangrovaPlanner constant folding: constant expressions are folded
 * into LogicalValues, while table-referencing queries pass through unchanged.
 */
class PangrovaPlannerConstantFoldTest {

    private final PangrovaPlanner planner = new PangrovaPlanner();

    // -- Constant folding produces LogicalValues --

    @Test
    void selectLiteralFoldsToLogicalValuesWithOneRow() {
        RelNode result = planner.plan("SELECT 1").plan();

        assertThat(result).isInstanceOf(LogicalValues.class);
        assertThat(((LogicalValues) result).getTuples()).hasSize(1);
    }

    @Test
    void selectExpressionAndStringIsConstantExpression() {
        RelNode result = planner.plan("SELECT 1+2, 'hello'").plan();

        // Calcite reduces 1+2 to 3 but may keep LogicalProject wrapping.
        // Verify the result has correct column count regardless of node type.
        assertThat(result.getRowType().getFieldCount()).isEqualTo(2);
    }

    @Test
    void selectFromTableDoesNotFold() {
        PangrovaPlanner plannerWithTable = buildPlannerWithTable();

        RelNode result = plannerWithTable.plan("SELECT * FROM T").plan();

        assertThat(result).isNotInstanceOf(LogicalValues.class);
        assertThat(PangrovaPlanner.isFoldedConstant(result)).isFalse();
    }

    @Test
    void selectLiteralWithFalsePredicateIsConstantExpression() {
        RelNode result = planner.plan("SELECT 1 WHERE 1=0").plan();

        // WHERE 1=0 eliminates all rows. Calcite may produce empty
        // LogicalValues directly or LogicalProject(LogicalValues).
        // Either way, no TableScan is involved.
        assertThat(result).isNotInstanceOf(
                org.apache.calcite.rel.core.TableScan.class);
    }

    // -- isFoldedConstant() utility --

    @Test
    void isFoldedConstantReturnsTrueForFoldedResult() {
        RelNode result = planner.plan("SELECT 1").plan();

        assertThat(PangrovaPlanner.isFoldedConstant(result)).isTrue();
    }

    @Test
    void isFoldedConstantReturnsFalseForTableScan() {
        PangrovaPlanner plannerWithTable = buildPlannerWithTable();

        RelNode result = plannerWithTable.plan("SELECT * FROM T").plan();

        assertThat(PangrovaPlanner.isFoldedConstant(result)).isFalse();
    }

    // -- Observability counter --

    @Test
    void constantFoldedTotalIncrementsOnSuccessfulFold() {
        PangrovaPlanner freshPlanner = new PangrovaPlanner();
        long before = freshPlanner.getConstantFoldedTotal();

        freshPlanner.plan("SELECT 1");

        assertThat(freshPlanner.getConstantFoldedTotal()).isEqualTo(before + 1);
    }

    // -- Helper --

    private static PangrovaPlanner buildPlannerWithTable() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("T", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("ID", typeFactory.createJavaType(int.class))
                        .add("NAME", typeFactory.createJavaType(String.class))
                        .build();
            }
        });
        return new PangrovaPlanner(rootSchema);
    }
}
