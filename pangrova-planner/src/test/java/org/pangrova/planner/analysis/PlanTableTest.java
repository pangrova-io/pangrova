package org.pangrova.planner.analysis;

import org.pangrova.planner.metadata.PlanTable;

import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.plan.RelOptTable;
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlanTableTest {
    @Test
    void returnsOnlyTheExactTableRetainedByTheCompiledRelation() {
        var planner = planner();
        var scan = scan(planner.plan("SELECT id FROM documents").plan());
        var modify = (TableModify) planner.plan("INSERT INTO documents VALUES (1)").plan();
        assertThat(PlanTable.require(scan)).isSameAs(scan.getTable());
        assertThat(PlanTable.require(modify)).isSameAs(modify.getTable());
    }

    @Test
    void missingAndNonTableRelationsFailWithPlannerValidationBeforeMetadataAccess() {
        var constant = new PangrovaPlanner().plan("SELECT 1").plan();
        for (var relation : new RelNode[]{null, constant}) {
            assertThatThrownBy(() -> PlanTable.require(relation))
                    .isInstanceOfSatisfying(PangrovaException.class,
                            failure -> assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE));
        }
    }

    @Test
    void missingScanAndMutationTablesAreRejectedBeforeDereferencingMetadata() {
        var planner = planner();
        var retainedScan = scan(planner.plan("SELECT id FROM documents").plan());
        var retainedModify = (TableModify) planner.plan("INSERT INTO documents VALUES (1)").plan();
        for (var malformed : new RelNode[]{new MissingTableScan(retainedScan),
                new MissingTableMutation(retainedModify)}) {
            assertThatThrownBy(() -> PlanTable.require(malformed))
                    .isInstanceOfSatisfying(PangrovaException.class,
                            failure -> assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE));
        }
    }

    private static PangrovaPlanner planner() {
        var schema = CalciteSchema.createRootSchema(false);
        schema.plus().add("documents", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.INTEGER).build();
            }
        });
        return new PangrovaPlanner(schema, java.util.List.of(""));
    }

    private static TableScan scan(RelNode node) {
        if (node instanceof TableScan scan) return scan;
        for (var input : node.getInputs()) {
            var scan = scan(input);
            if (scan != null) return scan;
        }
        return null;
    }

    private static final class MissingTableScan extends TableScan {
        private MissingTableScan(TableScan retained) {
            super(retained.getCluster(), retained.getTraitSet(), java.util.List.of(), retained.getTable());
        }

        @Override
        public RelOptTable getTable() {
            return null;
        }
    }

    private static final class MissingTableMutation extends TableModify {
        private MissingTableMutation(TableModify retained) {
            super(retained.getCluster(), retained.getTraitSet(), retained.getTable(),
                    retained.getCatalogReader(), retained.getInput(), retained.getOperation(),
                    retained.getUpdateColumnList(), retained.getSourceExpressionList(), retained.isFlattened());
        }

        @Override
        public RelOptTable getTable() {
            return null;
        }
    }
}
