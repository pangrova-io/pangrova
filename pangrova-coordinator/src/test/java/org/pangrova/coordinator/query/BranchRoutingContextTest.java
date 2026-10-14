package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.BranchRoutingContext;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies BranchRoutingContext session state management for per-table branch routing.
 */
class BranchRoutingContextTest {

    @Test
    void empty_hasNoRouting() {
        BranchRoutingContext ctx = BranchRoutingContext.empty();

        assertThat(ctx.hasRouting()).isFalse();
        assertThat(ctx.allRoutings()).isEmpty();
        assertThat(ctx.getBranch("catalog.schema.table")).isEmpty();
    }

    @Test
    void withBranch_addsRouting() {
        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("catalog.schema.knowledge_base", "agent_session_42");

        assertThat(ctx.hasRouting()).isTrue();
        assertThat(ctx.getBranch("catalog.schema.knowledge_base"))
                .hasValue("agent_session_42");
    }

    @Test
    void withBranch_multipleTablesIndependent() {
        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("cat.s.table_a", "branch_1")
                .withBranch("cat.s.table_b", "branch_2");

        assertThat(ctx.getBranch("cat.s.table_a")).hasValue("branch_1");
        assertThat(ctx.getBranch("cat.s.table_b")).hasValue("branch_2");
        assertThat(ctx.allRoutings()).hasSize(2);
    }

    @Test
    void withBranch_overridesPreviousRouting() {
        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "old_branch")
                .withBranch("cat.s.table", "new_branch");

        assertThat(ctx.getBranch("cat.s.table")).hasValue("new_branch");
        assertThat(ctx.allRoutings()).hasSize(1);
    }

    @Test
    void withoutBranch_removesRouting() {
        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("cat.s.table_a", "branch_1")
                .withBranch("cat.s.table_b", "branch_2")
                .withoutBranch("cat.s.table_a");

        assertThat(ctx.getBranch("cat.s.table_a")).isEmpty();
        assertThat(ctx.getBranch("cat.s.table_b")).hasValue("branch_2");
        assertThat(ctx.allRoutings()).hasSize(1);
    }

    @Test
    void withoutBranch_lastRemovalReturnsEmpty() {
        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "branch_1")
                .withoutBranch("cat.s.table");

        assertThat(ctx.hasRouting()).isFalse();
        assertThat(ctx.allRoutings()).isEmpty();
    }

    @Test
    void withoutBranch_nonExistentTableReturnsSameInstance() {
        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "branch_1");

        BranchRoutingContext result = ctx.withoutBranch("cat.s.other_table");

        assertThat(result).isSameAs(ctx);
    }

    @Test
    void toSessionProperties_serializesWithPrefix() {
        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("cat.s.table_a", "branch_1")
                .withBranch("cat.s.table_b", "branch_2");

        Map<String, String> props = ctx.toSessionProperties();

        assertThat(props).containsEntry("branch.routing.cat.s.table_a", "branch_1");
        assertThat(props).containsEntry("branch.routing.cat.s.table_b", "branch_2");
        assertThat(props).hasSize(2);
    }

    @Test
    void toSessionProperties_emptyReturnsEmptyMap() {
        Map<String, String> props = BranchRoutingContext.empty().toSessionProperties();

        assertThat(props).isEmpty();
    }

    @Test
    void fromSessionProperties_extractsBranchEntries() {
        Map<String, String> props = Map.of(
                "user", "alice",
                "catalog", "iceberg_catalog",
                "branch.routing.cat.s.table_a", "branch_1",
                "branch.routing.cat.s.table_b", "branch_2"
        );

        BranchRoutingContext ctx = BranchRoutingContext.fromSessionProperties(props);

        assertThat(ctx.hasRouting()).isTrue();
        assertThat(ctx.getBranch("cat.s.table_a")).hasValue("branch_1");
        assertThat(ctx.getBranch("cat.s.table_b")).hasValue("branch_2");
    }

    @Test
    void fromSessionProperties_noBranchEntriesReturnsEmpty() {
        Map<String, String> props = Map.of("user", "alice", "catalog", "iceberg_catalog");

        BranchRoutingContext ctx = BranchRoutingContext.fromSessionProperties(props);

        assertThat(ctx.hasRouting()).isFalse();
    }

    @Test
    void fromSessionProperties_nullReturnsEmpty() {
        BranchRoutingContext ctx = BranchRoutingContext.fromSessionProperties(null);

        assertThat(ctx.hasRouting()).isFalse();
    }

    @Test
    void fromSessionProperties_emptyMapReturnsEmpty() {
        BranchRoutingContext ctx = BranchRoutingContext.fromSessionProperties(Map.of());

        assertThat(ctx.hasRouting()).isFalse();
    }

    @Test
    void roundTrip_serializeAndDeserialize() {
        BranchRoutingContext original = BranchRoutingContext.empty()
                .withBranch("cat.s.table_a", "branch_1")
                .withBranch("cat.s.table_b", "branch_2");

        Map<String, String> serialized = original.toSessionProperties();
        BranchRoutingContext restored = BranchRoutingContext.fromSessionProperties(serialized);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    void mergeIntoSessionProperties_addsRoutingToExisting() {
        Map<String, String> original = Map.of("user", "alice", "catalog", "iceberg_catalog");
        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "branch_1");

        Map<String, String> merged = ctx.mergeIntoSessionProperties(original);

        assertThat(merged).containsEntry("user", "alice");
        assertThat(merged).containsEntry("catalog", "iceberg_catalog");
        assertThat(merged).containsEntry("branch.routing.cat.s.table", "branch_1");
    }

    @Test
    void mergeIntoSessionProperties_replacesExistingBranchEntries() {
        Map<String, String> original = new HashMap<>();
        original.put("user", "alice");
        original.put("branch.routing.cat.s.table", "old_branch");

        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "new_branch");

        Map<String, String> merged = ctx.mergeIntoSessionProperties(original);

        assertThat(merged).containsEntry("user", "alice");
        assertThat(merged).containsEntry("branch.routing.cat.s.table", "new_branch");
        assertThat(merged).hasSize(2);
    }

    @Test
    void mergeIntoSessionProperties_emptyContextRemovesBranchEntries() {
        Map<String, String> original = new HashMap<>();
        original.put("user", "alice");
        original.put("branch.routing.cat.s.table", "old_branch");

        BranchRoutingContext ctx = BranchRoutingContext.empty();

        Map<String, String> merged = ctx.mergeIntoSessionProperties(original);

        assertThat(merged).containsEntry("user", "alice");
        assertThat(merged).doesNotContainKey("branch.routing.cat.s.table");
        assertThat(merged).hasSize(1);
    }

    @Test
    void mergeIntoSessionProperties_nullOriginalReturnsOnlyBranchEntries() {
        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "branch_1");

        Map<String, String> merged = ctx.mergeIntoSessionProperties(null);

        assertThat(merged).containsEntry("branch.routing.cat.s.table", "branch_1");
        assertThat(merged).hasSize(1);
    }

    @Test
    void mergeIntoSessionProperties_noChangesReturnsSameMap() {
        Map<String, String> original = Map.of("user", "alice");
        BranchRoutingContext ctx = BranchRoutingContext.empty();

        Map<String, String> merged = ctx.mergeIntoSessionProperties(original);

        assertThat(merged).isSameAs(original);
    }

    @Test
    void immutability_withBranchDoesNotMutateOriginal() {
        BranchRoutingContext original = BranchRoutingContext.empty()
                .withBranch("cat.s.table_a", "branch_1");

        original.withBranch("cat.s.table_b", "branch_2");

        assertThat(original.allRoutings()).hasSize(1);
        assertThat(original.getBranch("cat.s.table_b")).isEmpty();
    }

    @Test
    void immutability_withoutBranchDoesNotMutateOriginal() {
        BranchRoutingContext original = BranchRoutingContext.empty()
                .withBranch("cat.s.table_a", "branch_1")
                .withBranch("cat.s.table_b", "branch_2");

        original.withoutBranch("cat.s.table_a");

        assertThat(original.allRoutings()).hasSize(2);
    }

    @Test
    void equality_sameRoutingsAreEqual() {
        BranchRoutingContext a = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "branch_1");
        BranchRoutingContext b = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "branch_1");

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void equality_differentRoutingsAreNotEqual() {
        BranchRoutingContext a = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "branch_1");
        BranchRoutingContext b = BranchRoutingContext.empty()
                .withBranch("cat.s.table", "branch_2");

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void propagationToWorker_sessionPropertiesFlowThroughTaskRequest() {
        // Simulates the full propagation path:
        // 1. Client sets USE BRANCH -> BranchRoutingContext created
        // 2. Context merged into CoordinatorSessionContext.sessionProperties
        // 3. TaskRequestFactory passes sessionProperties to SubmitTaskRequest
        // 4. Worker extracts BranchRoutingContext from received sessionProperties

        BranchRoutingContext clientSide = BranchRoutingContext.empty()
                .withBranch("iceberg_catalog.prod.knowledge_base", "agent_session_42");

        // Coordinator merges into session properties
        Map<String, String> sessionProps = clientSide.mergeIntoSessionProperties(
                Map.of("user", "alice", "catalog", "iceberg_catalog"));

        // Worker receives sessionProperties and extracts routing
        BranchRoutingContext workerSide = BranchRoutingContext.fromSessionProperties(sessionProps);

        assertThat(workerSide.getBranch("iceberg_catalog.prod.knowledge_base"))
                .hasValue("agent_session_42");
    }
}
