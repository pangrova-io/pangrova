package org.pangrova.planner.analysis;

import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CteCompilationContractTest {
    @Test
    void repeatedCteRetainsEveryRealSourceThroughNativeSerialization() throws Exception {
        var compiler = compiler();
        var plan = Plan.parseFrom(compiler.compile("WITH x AS (SELECT id FROM documents WHERE id > 0) "
                + "SELECT * FROM x UNION ALL SELECT * FROM x"));
        assertThat(plan.getRelationsCount()).isEqualTo(1);
        assertThat(readCount(plan.getRelations(0).getRoot().getInput())).isEqualTo(2);
        assertThat(plan.getRelations(0).getRoot().getNamesList()).containsExactly("id");
    }

    @Test
    void repeatedAndSingleUseQueriesOnTheSameCompilerRetainTheirOwnSources() throws Exception {
        var compiler = compiler();
        for (int index = 0; index < 5; index++) {
            var repeated = Plan.parseFrom(compiler.compile("WITH x AS (SELECT id FROM documents) "
                    + "SELECT * FROM x UNION ALL SELECT * FROM x"));
            var single = Plan.parseFrom(compiler.compile("SELECT id FROM documents WHERE id = 7"));
            assertThat(readCount(repeated.getRelations(0).getRoot().getInput())).isEqualTo(2);
            assertThat(readCount(single.getRelations(0).getRoot().getInput())).isEqualTo(1);
        }
    }

    private static SqlCompiler compiler() {
        var schema = CalciteSchema.createRootSchema(false);
        schema.plus().add("documents", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.INTEGER).build();
            }

            @Override
            public Statistic getStatistic() {
                return Statistics.of(10_000.0, List.of());
            }
        });
        return new SqlCompiler(new PangrovaPlanner(schema, List.of("")),
                new SubstraitConverter(), new SubstraitPlanSerializer());
    }

    private static int readCount(Rel relation) {
        return switch (relation.getRelTypeCase()) {
            case READ -> {
                assertThat(relation.getRead().getNamedTable().getNamesList()).containsExactly("documents");
                yield 1;
            }
            case PROJECT -> readCount(relation.getProject().getInput());
            case FILTER -> readCount(relation.getFilter().getInput());
            case SET -> relation.getSet().getInputsList().stream()
                    .mapToInt(CteCompilationContractTest::readCount).sum();
            default -> throw new AssertionError("Unexpected CTE execution relation: " + relation.getRelTypeCase());
        };
    }
}
