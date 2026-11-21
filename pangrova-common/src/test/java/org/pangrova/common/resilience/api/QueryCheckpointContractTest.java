package org.pangrova.common.resilience.api;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract tests for {@link QueryCheckpoint} SPI — validates the StageCheckpoint
 * record invariants and the default canResumeStage() implementation.
 */
class QueryCheckpointContractTest {

    @Test
    void stageCheckpointRejectsBlankQueryId() {
        assertThatThrownBy(() -> new QueryCheckpoint.StageCheckpoint(
                "", "s-1", 4, 100, List.of(), System.currentTimeMillis()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queryId");
    }

    @Test
    void stageCheckpointRejectsBlankStageId() {
        assertThatThrownBy(() -> new QueryCheckpoint.StageCheckpoint(
                "q-1", "", 4, 100, List.of(), System.currentTimeMillis()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stageId");
    }

    @Test
    void stageCheckpointRejectsNegativePartitions() {
        assertThatThrownBy(() -> new QueryCheckpoint.StageCheckpoint(
                "q-1", "s-1", -1, 100, List.of(), System.currentTimeMillis()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outputPartitions");
    }

    @Test
    void stageCheckpointRejectsNegativeRows() {
        assertThatThrownBy(() -> new QueryCheckpoint.StageCheckpoint(
                "q-1", "s-1", 4, -1, List.of(), System.currentTimeMillis()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outputRows");
    }

    @Test
    void stageCheckpointHandlesNullShuffleHandles() {
        var cp = new QueryCheckpoint.StageCheckpoint(
                "q-1", "s-1", 4, 100, null, System.currentTimeMillis());
        assertThat(cp.shuffleHandles()).isEmpty();
    }

    @Test
    void stageCheckpointPreservesShuffleHandlesImmutably() {
        var handles = List.of("h-1", "h-2", "h-3");
        var cp = new QueryCheckpoint.StageCheckpoint(
                "q-1", "s-1", 3, 300, handles, System.currentTimeMillis());
        assertThat(cp.shuffleHandles()).containsExactly("h-1", "h-2", "h-3");
        // Verify immutability
        assertThatThrownBy(() -> cp.shuffleHandles().add("h-4"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void canResumeStageDefaultReturnsFalseWhenNoCheckpoint() {
        // Use a minimal implementation that always returns empty
        QueryCheckpoint qcp = new QueryCheckpoint() {
            @Override
            public void checkpointStage(StageCheckpoint checkpoint) {}

            @Override
            public java.util.Optional<StageCheckpoint> getStageCheckpoint(
                    String queryId, String stageId) {
                return java.util.Optional.empty();
            }

            @Override
            public List<StageCheckpoint> getQueryCheckpoints(String queryId) {
                return List.of();
            }

            @Override
            public void clearQueryCheckpoints(String queryId) {}
        };

        assertThat(qcp.canResumeStage("q-1", "s-1")).isFalse();
    }

    @Test
    void canResumeStageDefaultReturnsTrueWhenCheckpointExists() {
        var checkpoint = new QueryCheckpoint.StageCheckpoint(
                "q-1", "s-1", 4, 100, List.of("h-1"), System.currentTimeMillis());

        QueryCheckpoint qcp = new QueryCheckpoint() {
            @Override
            public void checkpointStage(StageCheckpoint cp) {}

            @Override
            public java.util.Optional<StageCheckpoint> getStageCheckpoint(
                    String queryId, String stageId) {
                if ("q-1".equals(queryId) && "s-1".equals(stageId)) {
                    return java.util.Optional.of(checkpoint);
                }
                return java.util.Optional.empty();
            }

            @Override
            public List<StageCheckpoint> getQueryCheckpoints(String queryId) {
                return "q-1".equals(queryId) ? List.of(checkpoint) : List.of();
            }

            @Override
            public void clearQueryCheckpoints(String queryId) {}
        };

        assertThat(qcp.canResumeStage("q-1", "s-1")).isTrue();
        assertThat(qcp.canResumeStage("q-1", "s-2")).isFalse();
    }
}
