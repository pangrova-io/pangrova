package org.pangrova.coordinator.query.execution;

import org.junit.jupiter.api.Test;
import org.pangrova.security.authz.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.planner.AuthorizedStageGraph;
import org.pangrova.planner.SerializedPlanFragment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthorizedRelationTest {

    @Test
    void copiesEveryMutableInputAndReturnedWireValue() {
        byte[] plan = {1};
        byte[] schema = {2};
        List<String> names = new ArrayList<>(List.of("id"));
        Map<String, byte[]> schemas = new HashMap<>(Map.of("catalog.schema.orders", schema));
        Map<String, PolicySnapshot> observations = new HashMap<>(
                Map.of("catalog.schema.orders", PolicySnapshot.empty()));
        var relation = new AuthorizedRelation(plan, List.of(), names, List.of("BIGINT"),
                "digest", true, schemas, observations, graph());
        plan[0] = 3;
        schema[0] = 4;
        names.set(0, "changed");
        schemas.clear();
        observations.clear();
        relation.planBytes()[0] = 5;
        relation.sourceSchemas().get("catalog.schema.orders")[0] = 6;
        assertThat(relation.planBytes()).containsExactly((byte) 1);
        assertThat(relation.sourceSchemas().get("catalog.schema.orders")).containsExactly((byte) 2);
        assertThat(relation.outputNames()).containsExactly("id");
        assertThat(relation.observedPolicies()).containsOnlyKeys("catalog.schema.orders");
        assertThatThrownBy(() -> relation.outputNames().add("other"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsMissingPlanOrMismatchedOutputTypes() {
        assertThatThrownBy(() -> new AuthorizedRelation(new byte[0], List.of(), List.of(), List.of(),
                "digest", true, Map.of(), Map.of(), graph())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AuthorizedRelation(new byte[] {1}, List.of(), List.of("id"), List.of(),
                "digest", true, Map.of(), Map.of(), graph())).isInstanceOf(IllegalArgumentException.class);
    }

    private static AuthorizedStageGraph graph() {
        byte[] canonicalPlan = new org.pangrova.planner.SqlCompiler().compile("SELECT 1 AS id");
        return new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(new AuthorizedStageGraph.SerializedStage(
                0, 1, "OUTPUT", new SerializedPlanFragment(canonicalPlan, List.of(), List.of()),
                List.of(), List.of("id"), List.of(), List.of(), 0, 0)), List.of());
    }
}
