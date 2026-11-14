package org.pangrova.planner.substrait;

import io.substrait.plan.Plan;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.ImmutableExecutionBehavior;
import io.substrait.plan.ImmutableRoot;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ImmutableAggregate;
import io.substrait.relation.Rel;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.api.invocation.AiInvocationPlanHint;
import org.pangrova.planner.api.retrieval.RetrievalTopKPlanHint;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConversionSnapshotOwnershipTest {
    @Test
    void conversionHintsCannotBeChangedAfterBinding() {
        var retrieval = new RetrievalTopKPlanHint(RetrievalTopKPlanHint.Kind.EXACT,
                "", 0, 10, RetrievalTopKPlanHint.ScoreDirection.LOWER_IS_BETTER,
                List.of(1), new byte[]{1});
        var invocation = new AiInvocationPlanHint("0", "ai_complete", "model",
                AiInvocationPlanHint.Operation.TEXT_GENERATION, false);
        var retrievals = new ArrayList<>(List.of(retrieval));
        var invocations = new ArrayList<>(List.of(invocation));
        var plan = ImmutablePlan.builder().executionBehavior(ImmutableExecutionBehavior.builder()
                .variableEvaluationMode(Plan.ExecutionBehavior.VariableEvaluationMode.PER_PLAN)
                .build()).build();
        var result = new ConversionResult(plan, retrievals, invocations);
        retrievals.clear();
        invocations.clear();
        assertThat(result.retrievalTopKHints()).containsExactly(retrieval);
        assertThat(result.aiInvocationHints()).containsExactly(invocation);
        assertThatThrownBy(() -> result.retrievalTopKHints().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> result.aiInvocationHints().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void canonicalDistinctMeasuresAreFrozenBeforeSerialization() {
        var input = new PangrovaPlanner().plan("SELECT COUNT(DISTINCT X), COUNT(DISTINCT Y) "
                + "FROM (SELECT 1 AS X, 2 AS Y UNION ALL SELECT 1 AS X, 3 AS Y) t");
        var plan = new SubstraitConverter().convert(input.plan());
        var aggregate = findAggregate(plan.getRoots().get(0).getInput());
        assertThat(aggregate).isNotNull();
        var measures = new ArrayList<>(aggregate.getMeasures());
        var frozen = ImmutableAggregate.builder().from(aggregate).measures(measures).build();
        var root = ImmutableRoot.builder().from(plan.getRoots().get(0)).input(frozen).build();
        var snapshot = ImmutablePlan.builder().from(plan).roots(List.of(root)).build();
        var serializer = new SubstraitPlanSerializer();
        var serialized = serializer.serialize(snapshot);
        measures.clear();
        assertThat(frozen.getMeasures()).containsExactlyElementsOf(aggregate.getMeasures());
        assertThat(serializer.serialize(snapshot)).isEqualTo(serialized);
        assertThatThrownBy(() -> frozen.getMeasures().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static Aggregate findAggregate(Rel relation) {
        if (relation instanceof Aggregate aggregate) return aggregate;
        for (Rel input : relation.getInputs()) {
            Aggregate aggregate = findAggregate(input);
            if (aggregate != null) return aggregate;
        }
        return null;
    }
}
