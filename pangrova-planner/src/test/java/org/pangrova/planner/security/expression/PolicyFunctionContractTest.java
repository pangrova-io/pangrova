package org.pangrova.planner.security.expression;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.analysis.PangrovaPlanner;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Security expressions use the same function contracts as ordinary executable SQL. */
class PolicyFunctionContractTest {
    @ParameterizedTest
    @ValueSource(strings = {"LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH"})
    void policyLengthAndSubstringUseNativeBigintContracts(String name) {
        var schema = CalciteSchema.createRootSchema(false);
        schema.add("contacts", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("phone", SqlTypeName.VARCHAR).nullable(true).build();
            }
        });
        var scan = scan(new PangrovaPlanner(schema).plan("SELECT * FROM contacts").plan());
        var mask = MaskExpressionParser.parse("CASE WHEN phone IS NULL THEN NULL WHEN " + name
                + "(phone) <= 4 THEN '***' ELSE '***' || SUBSTRING(phone FROM " + name + "(phone) - 3) END", scan);
        var filter = FilterExpressionParser.parse(name + "(phone) > 4", scan);
        for (var expression : java.util.List.of(mask, filter)) {
            var calls = new ArrayList<RexCall>();
            expression.accept(new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitCall(RexCall call) {
                    calls.add(call);
                    return super.visitCall(call);
                }
            });
            assertThat(calls).filteredOn(call -> call.getOperator().getName().equals(name)).isNotEmpty()
                    .allSatisfy(call -> assertThat(call.getType().getSqlTypeName()).isEqualTo(SqlTypeName.BIGINT));
            assertThat(calls).filteredOn(call -> call.getOperator().getName().equals("SUBSTRING"))
                    .allSatisfy(call -> assertThat(call.getOperands().get(1).getType().getSqlTypeName())
                            .isEqualTo(SqlTypeName.BIGINT));
        }
    }

    private static TableScan scan(RelNode node) {
        if (node instanceof TableScan scan) return scan;
        return scan(node.getInput(0));
    }
}
