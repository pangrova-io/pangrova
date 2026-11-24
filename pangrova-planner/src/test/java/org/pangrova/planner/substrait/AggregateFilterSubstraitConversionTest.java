/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.planner.substrait;

import io.substrait.expression.FieldReference;
import io.substrait.plan.Plan;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Rel;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.PlanResult;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL:2003 aggregate FILTER clause conversion to Substrait
 * {@code Measure.preMeasureFilter} (proto field {@code AggregateRel.Measure.filter}).
 */
class AggregateFilterSubstraitConversionTest {

    private PangrovaPlanner planner;
    private SubstraitConverter converter;

    @BeforeEach
    void setUp() {
        planner = buildPlannerWithTable();
        converter = new SubstraitConverter();
    }

    @Test
    void countStarWithFilterProducesPreMeasureFilter() {
        Aggregate agg = planAggregate(
                "SELECT COUNT(*) FILTER (WHERE B > 10) FROM T");

        assertThat(agg.getMeasures()).hasSize(1);
        Aggregate.Measure measure = agg.getMeasures().get(0);
        assertThat(measure.getPreMeasureFilter()).isPresent();
        // Calcite pushes the condition down as a boolean input column, so the
        // pre-measure filter must be a boolean field reference.
        assertThat(measure.getPreMeasureFilter().get())
                .isInstanceOf(FieldReference.class);
        assertThat(measure.getPreMeasureFilter().get().getType().nullable())
                .isNotNull();
    }

    @Test
    void filteredAndUnfilteredMeasuresCoexist() {
        Aggregate agg = planAggregate(
                "SELECT SUM(A) FILTER (WHERE B > 10), COUNT(*) FROM T");

        // Calcite expands SUM into SUM0 + COUNT (empty-group NULL semantics)
        // and copies the FILTER onto every derived measure, so measure count
        // varies. Assert semantics instead: filtered and unfiltered measures
        // coexist, and the filtered ones all reference the same boolean input.
        assertThat(agg.getMeasures())
                .anyMatch(m -> m.getPreMeasureFilter().isPresent())
                .anyMatch(m -> m.getPreMeasureFilter().isEmpty());
        // The measure derived from SUM(A) FILTER comes first and keeps its filter.
        assertThat(agg.getMeasures().get(0).getPreMeasureFilter()).isPresent();
    }

    @Test
    void filterWithGroupByProducesPreMeasureFilter() {
        Aggregate agg = planAggregate(
                "SELECT C, COUNT(*) FILTER (WHERE B > 10) FROM T GROUP BY C");

        assertThat(agg.getMeasures()).hasSize(1);
        assertThat(agg.getMeasures().get(0).getPreMeasureFilter()).isPresent();
    }

    @Test
    void filterWithGroupingSetsUsesItsInputColumn() {
        Aggregate agg = planAggregate(
                "SELECT A, COUNT(*) FILTER (WHERE B > 10) FROM T "
                        + "GROUP BY GROUPING SETS ((A), ())");

        assertThat(agg.getMeasures()).hasSize(1);
        Aggregate.Measure measure = agg.getMeasures().get(0);
        assertThat(measure.getPreMeasureFilter()).isPresent();
        assertThat(measure.getPreMeasureFilter().get())
                .isInstanceOf(FieldReference.class);
    }

    @Test
    void groupingSetsBindSparseInputOrdinalsWithoutACompactionProject() {
        var input = planner.plan("SELECT A, B, C, D, B > 10 AS MASK FROM T").plan();
        assertThat(input.getRowType().getFieldCount()).isEqualTo(5);
        var key = ImmutableBitSet.of(3);
        var maximum = AggregateCall.create(SqlParserPos.ZERO, SqlStdOperatorTable.MAX,
                false, false, false, List.of(), List.of(1), 4, null, RelCollations.EMPTY,
                input.getCluster().getTypeFactory().createTypeWithNullability(
                        input.getRowType().getFieldList().get(1).getType(), true), "MAX_B");
        var aggregate = LogicalAggregate.create(input, List.of(), key, List.of(key, ImmutableBitSet.of()),
                List.of(maximum));
        var converted = findAggregate(converter.convert(aggregate).getRoots().get(0).getInput());

        assertThat(converted).isNotNull();
        assertThat(converted.getInput().getRecordType().fields()).hasSize(5);
        var grouped = converted.getGroupings().stream()
                .flatMap(grouping -> grouping.getExpressions().stream()).toList();
        assertThat(grouped).hasSize(1);
        assertThat(ordinal((FieldReference) grouped.get(0))).isEqualTo(3);
        var measure = converted.getMeasures().get(0);
        assertThat(ordinal((FieldReference) measure.getFunction().arguments().get(0))).isOne();
        assertThat(ordinal((FieldReference) measure.getPreMeasureFilter().orElseThrow())).isEqualTo(4);
    }

    private static int ordinal(FieldReference reference) {
        return ((FieldReference.StructField) reference.segments().get(0)).offset();
    }

    @Test
    void singleDistinctWithFilterIsAllowed() {
        Aggregate agg = planAggregate(
                "SELECT COUNT(DISTINCT A) FILTER (WHERE B > 10) FROM T");

        assertThat(agg.getMeasures()).hasSize(1);
        Aggregate.Measure measure = agg.getMeasures().get(0);
        assertThat(measure.getPreMeasureFilter()).isPresent();
        assertThat(measure.getFunction().invocation())
                .isEqualTo(io.substrait.expression.Expression.AggregationInvocation.DISTINCT);
    }

    @Test
    void multiDistinctPreservesTheFilteredAndUnfilteredCanonicalMeasures() {
        Aggregate aggregate = planAggregate(
                "SELECT COUNT(DISTINCT A) FILTER (WHERE B > 10), COUNT(DISTINCT C) FROM T");
        assertThat(aggregate.getMeasures()).hasSize(2)
                .allMatch(measure -> measure.getFunction().invocation()
                        == io.substrait.expression.Expression.AggregationInvocation.DISTINCT);
        assertThat(aggregate.getMeasures().get(0).getPreMeasureFilter())
                .hasValueSatisfying(filter -> assertThat(filter).isInstanceOf(FieldReference.class));
        assertThat(aggregate.getMeasures().get(1).getPreMeasureFilter()).isEmpty();
        assertThat(aggregate.getExtension()).isEmpty();
    }

    @Test
    void multiDistinctWithFilterOnNonDistinctMeasureIsAllowed() {
        Aggregate agg = planAggregate(
                "SELECT COUNT(DISTINCT A), COUNT(DISTINCT C), "
                        + "SUM(B) FILTER (WHERE B > 10) FROM T");

        // Calcite may expand SUM into SUM0 + COUNT; assert semantics:
        // every DISTINCT measure carries no filter, and every non-DISTINCT
        // measure (all derived from SUM(B) FILTER) carries one.
        var distinctMeasures = agg.getMeasures().stream()
                .filter(m -> m.getFunction().invocation()
                        == io.substrait.expression.Expression.AggregationInvocation.DISTINCT)
                .toList();
        var nonDistinctMeasures = agg.getMeasures().stream()
                .filter(m -> m.getFunction().invocation()
                        != io.substrait.expression.Expression.AggregationInvocation.DISTINCT)
                .toList();
        assertThat(distinctMeasures).hasSize(2)
                .allMatch(m -> m.getPreMeasureFilter().isEmpty());
        assertThat(nonDistinctMeasures).isNotEmpty()
                .allMatch(m -> m.getPreMeasureFilter().isPresent());
    }

    private Aggregate planAggregate(String sql) {
        PlanResult result = planner.plan(sql);
        Plan plan = converter.convert(result.plan());
        Aggregate agg = findAggregate(plan.getRoots().get(0).getInput());
        assertThat(agg).as("plan must contain an Aggregate rel").isNotNull();
        return agg;
    }

    private static Aggregate findAggregate(Rel rel) {
        if (rel instanceof Aggregate agg) {
            return agg;
        }
        for (Rel input : rel.getInputs()) {
            Aggregate found = findAggregate(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static PangrovaPlanner buildPlannerWithTable() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("T", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("A", SqlTypeName.INTEGER)
                        .add("B", SqlTypeName.INTEGER)
                        .add("C", SqlTypeName.INTEGER)
                        .add("D", SqlTypeName.VARCHAR, 100)
                        .build();
            }
        });
        return new PangrovaPlanner(rootSchema);
    }
}
