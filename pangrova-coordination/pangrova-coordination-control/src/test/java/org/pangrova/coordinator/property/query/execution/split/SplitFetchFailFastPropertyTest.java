package org.pangrova.coordinator.property.query.execution.split;

import org.pangrova.coordinator.enums.SplitFetchOutcome;
import org.pangrova.coordinator.query.execution.split.QuerySplit;
import org.pangrova.protocol.v2.DataSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.split.SplitFetchResult;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property tests for SplitFetcher fail-fast semantics.
 *
 * <p>Verifies that connector split-source exceptions, timeouts, and limit
 * violations always propagate as {@link PangrovaException} instead of being
 * silently swallowed into empty split lists.</p>
 *
 * <p><b>Validates: Requirements A3.4</b></p>
 */
class SplitFetchFailFastPropertyTest {

    // ── Property 1: checkSplitFetchLimits throws on timeout ──

    @Property(tries = 10)
    void timeoutAlwaysThrows(@ForAll("splitCounts") int splitCount) {
        long expiredDeadline = System.nanoTime() - 1;

        assertThatThrownBy(() -> SplitFetcher.checkSplitFetchLimits(splitCount, expiredDeadline))
                .isInstanceOf(PangrovaException.class)
                .satisfies(ex -> {
                    PangrovaException ae = (PangrovaException) ex;
                    assertThat(ae.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_TIMEOUT);
                    assertThat(ae.getMessage()).contains(String.valueOf(splitCount));
                });
    }

    // ── Property 2: checkSplitFetchLimits throws on limit reached ──

    @Property(tries = 10)
    void limitReachedAlwaysThrows(@ForAll("overLimitCounts") int splitCount) {
        long futureDeadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);

        assertThatThrownBy(() -> SplitFetcher.checkSplitFetchLimits(splitCount, futureDeadline))
                .isInstanceOf(PangrovaException.class)
                .satisfies(ex -> {
                    PangrovaException ae = (PangrovaException) ex;
                    assertThat(ae.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED);
                    assertThat(ae.getMessage()).contains(String.valueOf(splitCount));
                });
    }

    // ── Property 3: checkSplitFetchLimits passes when within bounds ──

    @Property(tries = 10)
    void withinLimitsNeverThrows(@ForAll("underLimitCounts") int splitCount) {
        long futureDeadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);

        SplitFetcher.checkSplitFetchLimits(splitCount, futureDeadline);
        // no exception = pass
    }

    // ── Property 4: SplitFetchOutcome enum covers all terminal states ──

    @Property(tries = 10)
    void outcomeEnumHasExactlyThreeValues() {
        SplitFetchOutcome[] values = SplitFetchOutcome.values();
        assertThat(values).hasSize(3);
        assertThat(values).containsExactlyInAnyOrder(
                SplitFetchOutcome.SUCCESS,
                SplitFetchOutcome.FAILED,
                SplitFetchOutcome.LIMIT_REACHED);
    }

    // ── Property 5: SplitFetchResult factory methods produce correct outcomes ──

    @Property(tries = 10)
    void successResultCarriesSplitsAndNoFailure(@ForAll("splitCounts") int count) {
        List<QuerySplit> splits = createSplitWithPathsList(count);

        SplitFetchResult result = SplitFetchResult.success(splits);

        assertThat(result.outcome()).isEqualTo(SplitFetchOutcome.SUCCESS);
        assertThat(result.splits()).hasSize(count);
        assertThat(result.failureReason()).isNull();
    }

    @Property(tries = 10)
    void failedResultCarriesReasonAndEmptySplits(@ForAll("failureReasons") String reason) {
        SplitFetchResult result = SplitFetchResult.failed(reason);

        assertThat(result.outcome()).isEqualTo(SplitFetchOutcome.FAILED);
        assertThat(result.splits()).isEmpty();
        assertThat(result.failureReason()).isEqualTo(reason);
    }

    @Property(tries = 10)
    void limitReachedResultCarriesPartialSplitsAndReason(
            @ForAll("splitCounts") int count,
            @ForAll("failureReasons") String reason) {
        List<QuerySplit> splits = createSplitWithPathsList(count);

        SplitFetchResult result =
                SplitFetchResult.limitReached(splits, reason);

        assertThat(result.outcome()).isEqualTo(SplitFetchOutcome.LIMIT_REACHED);
        assertThat(result.splits()).hasSize(count);
        assertThat(result.failureReason()).isEqualTo(reason);
    }

    // ── Property 6: timeout takes precedence over limit ──

    @Property(tries = 10)
    void timeoutPrecedesLimitWhenBothViolated(@ForAll("overLimitCounts") int splitCount) {
        long expiredDeadline = System.nanoTime() - 1;

        assertThatThrownBy(() -> SplitFetcher.checkSplitFetchLimits(splitCount, expiredDeadline))
                .isInstanceOf(PangrovaException.class)
                .satisfies(ex -> {
                    PangrovaException ae = (PangrovaException) ex;
                    assertThat(ae.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_TIMEOUT);
                });
    }

    // ── Providers ──

    @Provide
    Arbitrary<Integer> splitCounts() {
        return Arbitraries.integers().between(0, 100);
    }

    @Provide
    Arbitrary<Integer> overLimitCounts() {
        return Arbitraries.integers().between(100_001, 100_050);
    }

    @Provide
    Arbitrary<Integer> underLimitCounts() {
        return Arbitraries.integers().between(0, 100_000);
    }

    @Provide
    Arbitrary<String> failureReasons() {
        return Arbitraries.strings().alpha().ofMinLength(5).ofMaxLength(50)
                .map(s -> "error: " + s);
    }

    // ── Helpers ──

    private static List<QuerySplit> createSplitWithPathsList(int count) {
        List<QuerySplit> splits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            splits.add(new QuerySplit(split("data-" + i), List.of()));
        }
        return splits;
    }

    private static DataSplit split(String id) {
        return DataSplit.newBuilder().setSplitId(id).build();
    }

}
