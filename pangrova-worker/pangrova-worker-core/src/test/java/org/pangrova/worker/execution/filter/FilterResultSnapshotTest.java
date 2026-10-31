package org.pangrova.worker.execution.filter;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilterResultSnapshotTest {
    @Test
    void unfilteredEncodedSplitMembershipIsCaptured() {
        var splits = new ArrayList<>(List.of("split-a", "split-b"));
        var result = new DynamicFilterApplicator().apply(splits, List.of(), 1, "query");
        splits.clear();
        assertThat(result.retainedSplits()).containsExactly("split-a", "split-b");
        assertThat(result.totalSplits()).isEqualTo(2);
        assertThat(result.filteredSplits()).isZero();
        assertThatThrownBy(() -> result.retainedSplits().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void emptyTypedSplitDecisionCannotGainCallerAddedMembership() {
        var splits = new ArrayList<org.pangrova.protocol.v2.DataSplit>();
        var result = ConstraintBasedSplitFilter.filter(splits, List.of(), "query", 1);
        splits.add(org.pangrova.protocol.v2.DataSplit.getDefaultInstance());
        assertThat(result.retainedSplits()).isEmpty();
        assertThat(result.totalSplits()).isZero();
        assertThat(result.skippedSplits()).isZero();
        assertThatThrownBy(() -> result.retainedSplits().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
