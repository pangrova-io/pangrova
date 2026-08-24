package org.pangrova.coordinator.property.dynamictable.eval;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Combinators;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.eval.AggregateEvaluator;
import org.pangrova.coordinator.dynamictable.eval.AggregateEvaluator.AggregateFunction;
import org.pangrova.coordinator.dynamictable.quota.RefreshMemoryQuota;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.pangrova.spi.streaming.CdcRow;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Property-based tests for {@link AggregateEvaluator}.
 *
 * <p><b>Validates: Requirements 11</b></p>
 */
class AggregateEvaluatorPropertyTest {

    private static final String TABLE = "source_table";
    private static final List<String> GROUP_KEYS = List.of("category");
    private static final List<AggregateFunction> AGGREGATIONS = List.of(
            new AggregateFunction(AggregateFunction.Type.SUM, "amount", "total_amount"),
            new AggregateFunction(AggregateFunction.Type.COUNT, "amount", "row_count")
    );

    /**
     * Property 1: INSERT then DELETE same row → aggregation result unchanged.
     *
     * <p>After inserting a set of rows and then deleting the exact same rows,
     * the aggregation state should return to its initial (empty) state.</p>
     */
    @Property(tries = 10)
    void insertThenDeleteSameRowsLeavesAggregationUnchanged(
            @ForAll("rowBatches") List<TestRow> rows) {

        AggregateEvaluator evaluator = newEvaluator();
        DynamicTableState state = testState();

        // Phase 1: INSERT all rows
        List<CdcEnvelope> inserts = rows.stream()
                .map(r -> insertEnvelope(r))
                .toList();
        evaluator.evaluate(state, ChangeResult.cdcBatch(inserts));

        // Phase 2: DELETE all the same rows
        List<CdcEnvelope> deletes = rows.stream()
                .map(r -> deleteEnvelope(r))
                .toList();
        evaluator.evaluate(state, ChangeResult.cdcBatch(deletes));

        // After INSERT + DELETE of same rows, all aggregation values should be zero
        var finalState = evaluator.currentState();
        for (var entry : finalState.entrySet()) {
            for (var aggEntry : entry.getValue().entrySet()) {
                assertEquals(0L, aggEntry.getValue().longValue(),
                        "Aggregation " + aggEntry.getKey()
                                + " for group " + entry.getKey()
                                + " should be zero after INSERT+DELETE");
            }
        }
    }

    /**
     * Property 2: UPDATE = DELETE old + INSERT new equivalence.
     *
     * <p>Applying an UPDATE event should produce the same aggregation state
     * as applying a DELETE of the old row followed by an INSERT of the new row.</p>
     */
    @Property(tries = 10)
    void updateEqualsDeleteOldPlusInsertNew(
            @ForAll("rowPairs") List<TestRowPair> pairs) {

        DynamicTableState state = testState();

        // Path A: apply UPDATE events
        AggregateEvaluator evalUpdate = newEvaluator();
        List<CdcEnvelope> updates = pairs.stream()
                .map(p -> updateEnvelope(p.before(), p.after()))
                .toList();
        evalUpdate.evaluate(state, ChangeResult.cdcBatch(updates));
        var stateUpdate = evalUpdate.currentState();

        // Path B: apply DELETE old + INSERT new
        AggregateEvaluator evalSplit = newEvaluator();
        List<CdcEnvelope> splitEvents = new ArrayList<>();
        for (TestRowPair pair : pairs) {
            splitEvents.add(deleteEnvelope(pair.before()));
            splitEvents.add(insertEnvelope(pair.after()));
        }
        evalSplit.evaluate(state, ChangeResult.cdcBatch(splitEvents));
        var stateSplit = evalSplit.currentState();

        assertEquals(stateUpdate, stateSplit,
                "UPDATE path and DELETE+INSERT path must produce identical state");
    }

    // ── Generators ──────────────────────────────────────────────────

    @Provide
    Arbitrary<List<TestRow>> rowBatches() {
        return testRowArbitrary().list().ofMinSize(1).ofMaxSize(20);
    }

    @Provide
    Arbitrary<List<TestRowPair>> rowPairs() {
        Arbitrary<TestRowPair> pair = Combinators.combine(
                testRowArbitrary(), testRowArbitrary()
        ).as((before, after) -> new TestRowPair(
                before,
                new TestRow(before.category(), after.amount())
        ));
        return pair.list().ofMinSize(1).ofMaxSize(20);
    }

    private Arbitrary<TestRow> testRowArbitrary() {
        Arbitrary<String> categories = Arbitraries.of("A", "B", "C");
        Arbitrary<Long> amounts = Arbitraries.longs().between(1, 1000);
        return Combinators.combine(categories, amounts).as(TestRow::new);
    }

    // ── Helpers ─────────────────────────────────────────────────────

    record TestRow(String category, long amount) {}
    record TestRowPair(TestRow before, TestRow after) {}

    private static AggregateEvaluator newEvaluator() {
        return new AggregateEvaluator(GROUP_KEYS, AGGREGATIONS, RefreshMemoryQuota.unlimited());
    }

    private static DynamicTableState testState() {
        Instant now = Instant.now();
        return new DynamicTableState(
                "test_dt", "tenant-1", DynamicTableLifecycleState.ACTIVE,
                DynamicTableRefreshMode.CDC, Duration.ofMinutes(5),
                "offset-0", "CDC_OFFSET",
                1, "{}", "admin", "coord-1", 1L,
                now, 0L, 0, now, now);
    }

    private static CdcEnvelope insertEnvelope(TestRow row) {
        return new CdcEnvelope(
                CdcOperation.INSERT, TABLE,
                null,
                CdcRow.of(Map.of("category", row.category(), "amount", row.amount())),
                Instant.now(), 1, Map.of());
    }

    private static CdcEnvelope deleteEnvelope(TestRow row) {
        return new CdcEnvelope(
                CdcOperation.DELETE, TABLE,
                CdcRow.of(Map.of("category", row.category(), "amount", row.amount())),
                null,
                Instant.now(), 1, Map.of());
    }

    private static CdcEnvelope updateEnvelope(TestRow before, TestRow after) {
        return new CdcEnvelope(
                CdcOperation.UPDATE, TABLE,
                CdcRow.of(Map.of("category", before.category(), "amount", before.amount())),
                CdcRow.of(Map.of("category", after.category(), "amount", after.amount())),
                Instant.now(), 1, Map.of());
    }
}
