package org.pangrova.planner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InlineInsertValuesNormalizerTest {
    @Test
    void packsLargeConstantArrayInsertIntoOneValuesAndOneCastProject() {
        var schema = CalciteSchema.createRootSchema(false);
        schema.plus().add("vectors", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.INTEGER)
                        .add("embedding", factory.createArrayType(
                                factory.createSqlType(SqlTypeName.REAL), -1)).build();
            }
        });
        var sql = new StringBuilder("INSERT INTO vectors VALUES ");
        for (int row = 0; row < 300; row++) {
            if (row > 0) sql.append(", ");
            sql.append('(').append(row).append(", ARRAY[0.125000, -0.875000])");
        }

        var modify = (LogicalTableModify) new PangrovaPlanner(schema, List.of(""))
                .plan(sql.toString()).plan();

        assertThat(modify.getInput()).isInstanceOf(LogicalProject.class);
        var values = (LogicalValues) ((LogicalProject) modify.getInput()).getInput();
        assertThat(values.getTuples()).hasSize(300);
        assertThat(values.getTuples().get(0).get(0).getValueAs(Integer.class)).isZero();
        assertThat(values.getTuples().get(299).get(0).getValueAs(Integer.class)).isEqualTo(299);
    }

    @Test
    void removesOnlyNullabilityCastsAroundDoubleLiterals() {
        var schema = CalciteSchema.createRootSchema(false);
        schema.plus().add("measurements", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("amount", SqlTypeName.DOUBLE).build();
            }
        });
        var modify = (LogicalTableModify) new PangrovaPlanner(schema, List.of(""))
                .plan("INSERT INTO measurements VALUES (0.0)").plan();
        var seed = (LogicalValues) modify.getInput();
        var rex = modify.getCluster().getRexBuilder();
        var type = seed.getRowType().getFieldList().get(0).getType();
        var branches = new ArrayList<org.apache.calcite.rel.RelNode>();
        for (int i = 0; i < 2; i++) {
            var literal = rex.makeApproxLiteral(BigDecimal.valueOf(i * 1.5));
            var cast = rex.makeCall(type,
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST, List.of(literal));
            branches.add(LogicalProject.create(seed, List.of(), List.of(cast), List.of("amount")));
        }
        var expanded = modify.copy(modify.getTraitSet(),
                List.of(LogicalUnion.create(branches, true)));

        var normalized = (LogicalTableModify) InlineInsertValuesNormalizer.normalize(expanded);

        assertThat(normalized.getInput()).isInstanceOf(LogicalValues.class);
        assertThat(((LogicalValues) normalized.getInput()).getTuples()).hasSize(2);
    }

    @Test
    void packsCastedConstantInsertBranchesWithoutChangingRowOrder() {
        var modify = insert();
        var seed = (LogicalValues) modify.getInput();
        var rex = modify.getCluster().getRexBuilder();
        var fieldType = seed.getRowType().getFieldList().get(0).getType();
        var branches = new ArrayList<org.apache.calcite.rel.RelNode>();
        for (int i = 0; i < 300; i++) {
            branches.add(LogicalProject.create(seed, List.of(),
                    List.of(rex.makeCast(fieldType,
                            rex.makeExactLiteral(BigDecimal.valueOf(i)))),
                    List.of("id")));
        }
        var union = LogicalUnion.create(branches, true);
        var expanded = modify.copy(modify.getTraitSet(), List.of(union));

        var normalized = (LogicalTableModify) InlineInsertValuesNormalizer.normalize(expanded);

        assertThat(normalized.getInput()).isInstanceOf(LogicalValues.class);
        var values = (LogicalValues) normalized.getInput();
        assertThat(values.getTuples()).hasSize(300);
        assertThat(values.getTuples().get(0).get(0).getValueAs(Integer.class)).isZero();
        assertThat(values.getTuples().get(299).get(0).getValueAs(Integer.class)).isEqualTo(299);
    }

    @Test
    void leavesNonConstantBranchesUntouched() {
        var modify = insert();
        var seed = (LogicalValues) modify.getInput();
        var rex = modify.getCluster().getRexBuilder();
        var dynamic = LogicalProject.create(seed, List.of(),
                List.of(rex.makeInputRef(seed, 0)), List.of("id"));
        var union = LogicalUnion.create(List.of(dynamic, dynamic), true);
        var expanded = modify.copy(modify.getTraitSet(), List.of(union));

        assertThat(InlineInsertValuesNormalizer.normalize(expanded)).isSameAs(expanded);
    }

    @Test
    void keepsValueChangingCastsForNormalExecution() {
        var modify = insert();
        var seed = (LogicalValues) modify.getInput();
        var rex = modify.getCluster().getRexBuilder();
        var target = seed.getRowType().getFieldList().get(0).getType();
        var cast = rex.makeCall(target,
                org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST,
                List.of(rex.makeExactLiteral(new BigDecimal("1.5"))));
        var branch = LogicalProject.create(seed, List.of(), List.of(cast), List.of("id"));
        var expanded = modify.copy(modify.getTraitSet(),
                List.of(LogicalUnion.create(List.of(branch, branch), true)));

        var normalized = (LogicalTableModify) InlineInsertValuesNormalizer.normalize(expanded);
        assertThat(normalized.getInput()).isInstanceOf(LogicalProject.class);
        var packed = (LogicalProject) normalized.getInput();
        assertThat(packed.getProjects().get(0)).isInstanceOf(org.apache.calcite.rex.RexCall.class)
                .satisfies(expression -> assertThat(((org.apache.calcite.rex.RexCall) expression).getKind())
                        .isEqualTo(org.apache.calcite.sql.SqlKind.CAST));
        assertThat(((LogicalValues) packed.getInput()).getTuples()).hasSize(2);
    }

    private static LogicalTableModify insert() {
        var schema = CalciteSchema.createRootSchema(false);
        schema.plus().add("documents", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.INTEGER).build();
            }
        });
        return (LogicalTableModify) new PangrovaPlanner(schema, List.of(""))
                .plan("INSERT INTO documents VALUES (0)").plan();
    }
}
