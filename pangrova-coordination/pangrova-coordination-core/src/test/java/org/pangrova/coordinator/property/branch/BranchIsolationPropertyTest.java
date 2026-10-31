package org.pangrova.coordinator.property.branch;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.coordinator.query.session.BranchRoutingContext;

/**
 * Property-based tests for branch isolation (P3).
 *
 * <p>Validates that writes on a branch do not appear in reads on main.
 * The BranchRoutingContext is the session-level mechanism that routes queries
 * to branches. A context with no routing represents the main branch perspective.
 * Branch isolation means: routing to branch X on table T does not affect the
 * main branch view of T (represented by an empty context or a context without
 * routing for T).</p>
 *
 * <p><b>Validates: Requirements P3</b></p>
 */
class BranchIsolationPropertyTest {

    @Provide
    Arbitrary<String> qualifiedTableNames() {
        Arbitrary<String> catalog = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                .map(String::toLowerCase);
        Arbitrary<String> schema = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                .map(String::toLowerCase);
        Arbitrary<String> table = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(12)
                .map(String::toLowerCase);
        return Combinators.combine(catalog, schema, table)
                .as((c, s, t) -> c + "." + s + "." + t);
    }

    @Provide
    Arbitrary<String> branchNames() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20)
                .map(s -> "branch_" + s.toLowerCase());
    }

    /**
     * P3: After routing table T to branch B, the main branch context (empty)
     * has no routing for T. Branch writes are invisible from main.
     */
    @Property(tries = 50)
    void branchRoutingDoesNotAffectMainContext(
            @ForAll("qualifiedTableNames") String tableName,
            @ForAll("branchNames") String branchName) {

        BranchRoutingContext mainContext = BranchRoutingContext.empty();
        BranchRoutingContext branchContext = mainContext.withBranch(tableName, branchName);

        // Branch context sees the branch
        assertThat(branchContext.getBranch(tableName)).hasValue(branchName);

        // Main context remains unaffected (isolation)
        assertThat(mainContext.getBranch(tableName)).isEmpty();
        assertThat(mainContext.hasRouting()).isFalse();
    }

    /**
     * P3: Multiple tables can be routed to different branches independently.
     * Routing table A to branch X does not affect table B's routing.
     */
    @Property(tries = 50)
    void branchRoutingIsPerTableIsolated(
            @ForAll("qualifiedTableNames") String tableA,
            @ForAll("qualifiedTableNames") String tableB,
            @ForAll("branchNames") String branchA,
            @ForAll("branchNames") String branchB) {

        // Skip when tables are the same (degenerate case)
        if (tableA.equals(tableB)) {
            return;
        }

        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch(tableA, branchA);

        // Table A is routed to branch A
        assertThat(ctx.getBranch(tableA)).hasValue(branchA);

        // Table B has no routing (reads from main)
        assertThat(ctx.getBranch(tableB)).isEmpty();

        // Adding routing for table B does not affect table A
        BranchRoutingContext both = ctx.withBranch(tableB, branchB);
        assertThat(both.getBranch(tableA)).hasValue(branchA);
        assertThat(both.getBranch(tableB)).hasValue(branchB);

        // Original context is unchanged (immutability ensures isolation)
        assertThat(ctx.getBranch(tableB)).isEmpty();
    }

    /**
     * P3: Removing branch routing (USE BRANCH MAIN) restores main visibility.
     * After withoutBranch, the table reads from main again.
     */
    @Property(tries = 50)
    void removingBranchRoutingRestoresMainVisibility(
            @ForAll("qualifiedTableNames") String tableName,
            @ForAll("branchNames") String branchName) {

        BranchRoutingContext branched = BranchRoutingContext.empty()
                .withBranch(tableName, branchName);

        // Verify branch is active
        assertThat(branched.getBranch(tableName)).hasValue(branchName);

        // Remove routing (USE BRANCH MAIN)
        BranchRoutingContext restored = branched.withoutBranch(tableName);

        // Table now reads from main (no routing)
        assertThat(restored.getBranch(tableName)).isEmpty();
        assertThat(restored.hasRouting()).isFalse();
    }

    /**
     * P3: Session property serialization preserves branch isolation.
     * Branch routing serialized to session properties and deserialized back
     * maintains the same isolation semantics.
     */
    @Property(tries = 50)
    void sessionPropertyRoundTripPreservesIsolation(
            @ForAll("qualifiedTableNames") String tableName,
            @ForAll("branchNames") String branchName) {

        BranchRoutingContext original = BranchRoutingContext.empty()
                .withBranch(tableName, branchName);

        // Serialize to session properties (propagated to Workers)
        Map<String, String> props = original.toSessionProperties();

        // Deserialize on Worker side
        BranchRoutingContext restored = BranchRoutingContext.fromSessionProperties(props);

        // Isolation preserved: same branch visible
        assertThat(restored.getBranch(tableName)).hasValue(branchName);
        assertThat(restored).isEqualTo(original);

        // Main context (empty properties) still sees no routing
        BranchRoutingContext mainOnWorker = BranchRoutingContext.fromSessionProperties(Map.of());
        assertThat(mainOnWorker.getBranch(tableName)).isEmpty();
    }

    /**
     * P3: Branch routing merged into session properties does not leak
     * into non-branch properties. Only prefixed keys carry branch state.
     */
    @Property(tries = 50)
    void branchRoutingDoesNotLeakIntoNonBranchProperties(
            @ForAll("qualifiedTableNames") String tableName,
            @ForAll("branchNames") String branchName) {

        BranchRoutingContext ctx = BranchRoutingContext.empty()
                .withBranch(tableName, branchName);

        Map<String, String> original = Map.of("user", "alice", "catalog", "iceberg");
        Map<String, String> merged = ctx.mergeIntoSessionProperties(original);

        // Non-branch properties preserved
        assertThat(merged).containsEntry("user", "alice");
        assertThat(merged).containsEntry("catalog", "iceberg");

        // Branch routing added with prefix
        assertThat(merged).containsEntry("branch.routing." + tableName, branchName);

        // Extracting from merged: only branch routing is extracted
        BranchRoutingContext extracted = BranchRoutingContext.fromSessionProperties(merged);
        assertThat(extracted.getBranch(tableName)).hasValue(branchName);
        assertThat(extracted.allRoutings()).hasSize(1);
    }
}
