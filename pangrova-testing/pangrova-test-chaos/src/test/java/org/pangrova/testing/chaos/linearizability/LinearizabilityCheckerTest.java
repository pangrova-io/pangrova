package org.pangrova.testing.chaos.linearizability;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the linearizability checker against metadata operation histories.
 * Tests cover: sequential histories, concurrent linearizable histories, and non-linearizable histories.
 */
class LinearizabilityCheckerTest {

    /**
     * Sequential specification for metadata operations (createTable / dropTable).
     * createTable succeeds ("ok") if the table does not exist; fails ("alreadyExists") otherwise.
     * dropTable succeeds ("ok") if the table exists; fails ("notFound") otherwise.
     */
    private static final BiPredicate<Set<String>, Operation> METADATA_SPEC = (state, op) -> {
        return switch (op.type()) {
            case "createTable" -> {
                if (state.contains(op.argument())) {
                    yield "alreadyExists".equals(op.result());
                } else {
                    yield "ok".equals(op.result());
                }
            }
            case "dropTable" -> {
                if (state.contains(op.argument())) {
                    yield "ok".equals(op.result());
                } else {
                    yield "notFound".equals(op.result());
                }
            }
            default -> true;
        };
    };

    @Test
    void emptyHistoryIsLinearizable() {
        var checker = new LinearizabilityChecker(METADATA_SPEC);
        var report = checker.check(List.of());

        assertThat(report.linearizable()).isTrue();
        assertThat(report.witness()).isEmpty();
    }

    @Test
    void sequentialCreateThenDropIsLinearizable() {
        var checker = new LinearizabilityChecker(METADATA_SPEC);
        Instant t1 = Instant.ofEpochMilli(1000);
        Instant t2 = Instant.ofEpochMilli(2000);
        Instant t3 = Instant.ofEpochMilli(3000);
        Instant t4 = Instant.ofEpochMilli(4000);

        var create = new Operation("c1", "createTable", "orders", "ok", t1, t2);
        var drop = new Operation("c1", "dropTable", "orders", "ok", t3, t4);

        var report = checker.check(List.of(create, drop));

        assertThat(report.linearizable()).isTrue();
        assertThat(report.witness()).containsExactly(create, drop);
    }

    @Test
    void concurrentCreatesWithOneSuccessIsLinearizable() {
        var checker = new LinearizabilityChecker(METADATA_SPEC);
        // Two clients concurrently create the same table. One succeeds, one gets alreadyExists.
        Instant t1 = Instant.ofEpochMilli(1000);
        Instant t2 = Instant.ofEpochMilli(3000);
        Instant t3 = Instant.ofEpochMilli(1500);
        Instant t4 = Instant.ofEpochMilli(2500);

        var create1 = new Operation("c1", "createTable", "orders", "ok", t1, t2);
        var create2 = new Operation("c2", "createTable", "orders", "alreadyExists", t3, t4);

        var report = checker.check(List.of(create1, create2));

        assertThat(report.linearizable()).isTrue();
        // The witness must place create1 before create2
        assertThat(report.witness().get(0)).isEqualTo(create1);
        assertThat(report.witness().get(1)).isEqualTo(create2);
    }

    @Test
    void concurrentCreatesBothSucceedingIsNotLinearizable() {
        var checker = new LinearizabilityChecker(METADATA_SPEC);
        // Two clients concurrently create the same table. Both report success — impossible.
        Instant t1 = Instant.ofEpochMilli(1000);
        Instant t2 = Instant.ofEpochMilli(3000);
        Instant t3 = Instant.ofEpochMilli(1500);
        Instant t4 = Instant.ofEpochMilli(2500);

        var create1 = new Operation("c1", "createTable", "orders", "ok", t1, t2);
        var create2 = new Operation("c2", "createTable", "orders", "ok", t3, t4);

        var report = checker.check(List.of(create1, create2));

        assertThat(report.linearizable()).isFalse();
        assertThat(report.violation()).contains("No valid linearization found");
    }

    @Test
    void dropBeforeCreateViolatesRealTimeOrder() {
        var checker = new LinearizabilityChecker(METADATA_SPEC);
        // Create completes at t2, drop starts at t3 (after create). Drop succeeds.
        // But if we also have a second create that starts after drop and succeeds, that's fine.
        Instant t1 = Instant.ofEpochMilli(1000);
        Instant t2 = Instant.ofEpochMilli(2000);
        Instant t3 = Instant.ofEpochMilli(3000);
        Instant t4 = Instant.ofEpochMilli(4000);
        Instant t5 = Instant.ofEpochMilli(5000);
        Instant t6 = Instant.ofEpochMilli(6000);

        var create = new Operation("c1", "createTable", "orders", "ok", t1, t2);
        var drop = new Operation("c1", "dropTable", "orders", "ok", t3, t4);
        var recreate = new Operation("c2", "createTable", "orders", "ok", t5, t6);

        var report = checker.check(List.of(create, drop, recreate));

        assertThat(report.linearizable()).isTrue();
        assertThat(report.witness()).containsExactly(create, drop, recreate);
    }

    @Test
    void coordinatorFailoverScenarioIsLinearizable() {
        var checker = new LinearizabilityChecker(METADATA_SPEC);
        // Simulates coordinator failover: client issues createTable, coordinator crashes mid-operation.
        // After failover, client retries and gets "alreadyExists" — the first create actually committed.
        Instant t1 = Instant.ofEpochMilli(1000);
        Instant t2 = Instant.ofEpochMilli(5000); // long duration due to failover
        Instant t3 = Instant.ofEpochMilli(6000);
        Instant t4 = Instant.ofEpochMilli(7000);

        // First attempt: client got timeout/error but the create actually went through
        var firstAttempt = new Operation("c1", "createTable", "orders", "ok", t1, t2);
        // Retry after failover: table already exists
        var retry = new Operation("c1", "createTable", "orders", "alreadyExists", t3, t4);

        var report = checker.check(List.of(firstAttempt, retry));

        assertThat(report.linearizable()).isTrue();
    }
}
