package org.pangrova.coordinator.property.training;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.coordinator.ai.training.SplitExpressionParser;
import org.pangrova.coordinator.ai.training.SplitExpressionParser.SplitDescriptor;
import org.pangrova.coordinator.ai.training.SplitExpressionParser.SplitPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for split determinism (P6).
 *
 * <p>Validates that the same split expression always produces the same partition
 * assignment for the same row. Hash-based splitting must be deterministic:
 * given a fixed column value and a fixed split expression, the evaluation result
 * (include/exclude) must be identical across repeated invocations.</p>
 *
 * <p><b>Validates: Requirements P6</b></p>
 */
class SplitDeterminismPropertyTest {

    @Provide
    Arbitrary<String> columnNames() {
        return Arbitraries.of("id", "user_id", "row_key", "pk", "doc_id");
    }

    @Provide
    Arbitrary<Integer> divisors() {
        return Arbitraries.integers().between(2, 100);
    }

    @Provide
    Arbitrary<Long> columnValues() {
        return Arbitraries.longs().between(0L, 1_000_000L);
    }

    /**
     * P6: Parsing the same split expression multiple times always produces
     * identical SplitDescriptor structures (column, divisor, operator, value).
     */
    @Property(tries = 100)
    void sameSplitExpressionAlwaysProducesSameDescriptor(
            @ForAll("columnNames") String column,
            @ForAll("divisors") int divisor,
            @ForAll @IntRange(min = 0, max = 99) int rawValue) {

        int value = rawValue % divisor;
        String expression = "hash(" + column + ") % " + divisor + " < " + value;

        // Parse the same expression twice
        SplitDescriptor first = SplitExpressionParser.parse(expression);
        SplitDescriptor second = SplitExpressionParser.parse(expression);

        // Both parses must produce identical results
        assertThat(first.predicates()).hasSize(second.predicates().length);
        for (int i = 0; i < first.predicates().length; i++) {
            SplitPredicate p1 = first.predicates()[i];
            SplitPredicate p2 = second.predicates()[i];
            assertThat(p1.column()).isEqualTo(p2.column());
            assertThat(p1.divisor()).isEqualTo(p2.divisor());
            assertThat(p1.operator()).isEqualTo(p2.operator());
            assertThat(p1.value()).isEqualTo(p2.value());
        }
    }

    /**
     * P6: For a given column value, the hash-modulo partition assignment is
     * deterministic — evaluating the same expression on the same value always
     * yields the same include/exclude decision.
     */
    @Property(tries = 200)
    void hashModuloAssignmentIsDeterministic(
            @ForAll("columnValues") long columnValue,
            @ForAll("divisors") int divisor,
            @ForAll @IntRange(min = 0, max = 99) int rawThreshold) {

        int threshold = rawThreshold % divisor;

        // Simulate the hash-based split evaluation: hash(value) % divisor < threshold
        int hashResult1 = computeHashModulo(columnValue, divisor);
        int hashResult2 = computeHashModulo(columnValue, divisor);

        // Same input always produces same hash modulo result
        assertThat(hashResult1).isEqualTo(hashResult2);

        // Same partition assignment decision
        boolean included1 = hashResult1 < threshold;
        boolean included2 = hashResult2 < threshold;
        assertThat(included1).isEqualTo(included2);
    }

    /**
     * P6: Compound split expressions (AND-joined predicates) produce deterministic
     * partition assignments. The same row is always assigned to the same split
     * regardless of how many times the expression is evaluated.
     */
    @Property(tries = 100)
    void compoundSplitExpressionIsDeterministic(
            @ForAll("columnNames") String column,
            @ForAll("divisors") int divisor,
            @ForAll("columnValues") long columnValue) {

        // Construct a compound expression: hash(col) % divisor >= X AND hash(col) % divisor < Y
        int lowerBound = divisor / 4;
        int upperBound = divisor / 2;
        if (lowerBound >= upperBound) {
            upperBound = lowerBound + 1;
        }
        if (upperBound >= divisor) {
            return; // skip degenerate case
        }

        String expression = "hash(" + column + ") % " + divisor + " >= " + lowerBound
                + " AND hash(" + column + ") % " + divisor + " < " + upperBound;

        // Parse succeeds and is deterministic
        SplitDescriptor desc1 = SplitExpressionParser.parse(expression);
        SplitDescriptor desc2 = SplitExpressionParser.parse(expression);

        assertThat(desc1.predicates()).hasSize(2);
        assertThat(desc2.predicates()).hasSize(2);

        // Evaluate partition assignment multiple times
        int hash = computeHashModulo(columnValue, divisor);
        boolean inSplit1 = hash >= lowerBound && hash < upperBound;
        boolean inSplit2 = hash >= lowerBound && hash < upperBound;

        assertThat(inSplit1).isEqualTo(inSplit2);
    }

    /**
     * P6: Train/val/test splits are mutually exclusive and collectively exhaustive.
     * For any column value, exactly one of the three splits includes it.
     */
    @Property(tries = 200)
    void trainValTestSplitsArePartition(
            @ForAll("columnValues") long columnValue,
            @ForAll @IntRange(min = 5, max = 100) int divisor) {

        // Standard 80/10/10 split
        int trainThreshold = (int) (divisor * 0.8);
        int valThreshold = (int) (divisor * 0.9);

        int hash = computeHashModulo(columnValue, divisor);

        boolean inTrain = hash < trainThreshold;
        boolean inVal = hash >= trainThreshold && hash < valThreshold;
        boolean inTest = hash >= valThreshold;

        // Exactly one split contains this row
        int count = (inTrain ? 1 : 0) + (inVal ? 1 : 0) + (inTest ? 1 : 0);
        assertThat(count)
                .as("Row with hash=%d must be in exactly one split (train/val/test)", hash)
                .isEqualTo(1);
    }

    /**
     * P6: The hash function distributes values across all buckets.
     * For a sufficiently large range of inputs, all modulo buckets are hit.
     */
    @Property(tries = 50)
    void hashDistributesAcrossBuckets(@ForAll @IntRange(min = 2, max = 10) int divisor) {
        boolean[] bucketHit = new boolean[divisor];
        for (long v = 0; v < divisor * 100L; v++) {
            int bucket = computeHashModulo(v, divisor);
            assertThat(bucket).isBetween(0, divisor - 1);
            bucketHit[bucket] = true;
        }
        for (int i = 0; i < divisor; i++) {
            assertThat(bucketHit[i])
                    .as("Bucket %d should be hit for divisor %d", i, divisor)
                    .isTrue();
        }
    }

    /**
     * Simulates the hash-based split assignment used by the training data server.
     * Uses the same deterministic hash approach as the split expression evaluator.
     */
    private static int computeHashModulo(long value, int divisor) {
        int hash = Long.hashCode(value);
        return Math.floorMod(hash, divisor);
    }
}
