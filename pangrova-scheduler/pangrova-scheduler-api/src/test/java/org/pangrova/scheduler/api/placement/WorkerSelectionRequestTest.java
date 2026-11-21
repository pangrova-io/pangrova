package org.pangrova.scheduler.api.placement;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerSelectionRequestTest {
    @Test
    void requestOwnsItsImmutableCandidateSnapshot() {
        var candidates = new ArrayList<>(List.of(new WorkerSelectionCandidate("w", .1, .2)));
        var request = new WorkerSelectionRequest(candidates);
        candidates.clear();
        assertThat(request.candidates()).hasSize(1);
        assertThatThrownBy(() -> request.candidates().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void duplicateIdentitiesAreAmbiguousAndRejected() {
        assertThatThrownBy(() -> new WorkerSelectionRequest(List.of(
                new WorkerSelectionCandidate("w", .1, .2), new WorkerSelectionCandidate("w", .3, .4))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate worker");
    }

    @Test
    void invalidTelemetryCannotWinRanking() {
        for (double invalid : new double[]{Double.NaN, Double.POSITIVE_INFINITY, -.1, 1.1}) {
            assertThatThrownBy(() -> new WorkerSelectionCandidate("w", invalid, .1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new WorkerSelectionCandidate("w", .1, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new WorkerSelectionCandidate(" ", .1, .1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
