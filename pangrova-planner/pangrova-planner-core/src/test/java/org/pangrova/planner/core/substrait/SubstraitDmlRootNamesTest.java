package org.pangrova.planner.core.substrait;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.analysis.PangrovaPlanner;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SubstraitDmlRootNamesTest {

    @Test
    void insertUsesWrittenColumnsInsteadOfRowCountName() {
        var plan = convert(TableModify.Operation.INSERT);

        assertThat(plan.getRoots().get(0).getNames())
                .containsExactly("id", "title", "score");
    }

    @Test
    void deleteCopyOnWriteUsesSurvivingRowColumns() {
        var plan = convert(TableModify.Operation.DELETE);

        assertThat(plan.getRoots().get(0).getNames())
                .containsExactly("id", "title", "score");
    }

    @Test
    void mergeUpdateRequiresOneSourceMatchPerTarget() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("target_rows", table());
        schema.add("source_rows", table());
        RelNode merge = new PangrovaPlanner(schema).plan(
                "MERGE INTO target_rows t USING source_rows s ON t.id = s.id "
                        + "WHEN MATCHED THEN UPDATE SET title = s.title").plan();
        var proto = io.substrait.proto.Plan.parseFrom(
                new SubstraitPlanSerializer().serialize(
                        new SubstraitConverter().convert(merge)));

        assertThat(hasSingleMatchJoin(proto.getRelations(0).getRoot().getInput()))
                .isTrue();
    }

    private static io.substrait.plan.Plan convert(TableModify.Operation operation) {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("documents", table());
        RelNode input = new PangrovaPlanner(schema)
                .plan("SELECT * FROM documents").plan();
        TableScan scan = findScan(input);
        LogicalTableModify modify = LogicalTableModify.create(
                scan.getTable(), null, input, operation,
                null, null, false);
        return new SubstraitConverter().convertWithResult(
                modify, List.of()).plan();
    }

    private static AbstractTable table() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder()
                        .add("id", SqlTypeName.BIGINT)
                        .add("title", SqlTypeName.VARCHAR, 100)
                        .add("score", SqlTypeName.DOUBLE)
                        .build();
            }
        };
    }

    private static boolean hasSingleMatchJoin(io.substrait.proto.Rel rel) {
        return switch (rel.getRelTypeCase()) {
            case JOIN -> rel.getJoin().getType()
                    == io.substrait.proto.JoinRel.JoinType.JOIN_TYPE_LEFT_SINGLE
                    || hasSingleMatchJoin(rel.getJoin().getLeft())
                    || hasSingleMatchJoin(rel.getJoin().getRight());
            case PROJECT -> hasSingleMatchJoin(rel.getProject().getInput());
            case FILTER -> hasSingleMatchJoin(rel.getFilter().getInput());
            case SET -> rel.getSet().getInputsList().stream()
                    .anyMatch(SubstraitDmlRootNamesTest::hasSingleMatchJoin);
            default -> false;
        };
    }

    private static TableScan findScan(RelNode node) {
        if (node instanceof TableScan scan) {
            return scan;
        }
        for (RelNode input : node.getInputs()) {
            TableScan found = findScan(input);
            if (found != null) {
                return found;
            }
        }
        throw new IllegalStateException("Test input has no table scan");
    }
}
