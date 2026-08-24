package org.pangrova.resilience.checkpoint;

import org.pangrova.spi.resilience.QueryCheckpoint.StageCheckpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link InMemoryQueryCheckpoint}.
 */
class InMemoryQueryCheckpointTest {

    private InMemoryQueryCheckpoint checkpoint;

    @BeforeEach
    void setUp() {
        checkpoint = new InMemoryQueryCheckpoint();
    }

    @Test
    void checkpointAndRetrieveStage() {
        var cp = new StageCheckpoint("q-1", "s-1", 4, 1000,
                List.of("h-1", "h-2"), 100L);
        checkpoint.checkpointStage(cp);

        var result = checkpoint.getStageCheckpoint("q-1", "s-1");
        assertThat(result).isPresent();
        assertThat(result.get().queryId()).isEqualTo("q-1");
        assertThat(result.get().stageId()).isEqualTo("s-1");
        assertThat(result.get().outputPartitions()).isEqualTo(4);
        assertThat(result.get().outputRows()).isEqualTo(1000);
        assertThat(result.get().shuffleHandles()).containsExactly("h-1", "h-2");
    }

    @Test
    void getMissingStageReturnsEmpty() {
        assertThat(checkpoint.getStageCheckpoint("q-1", "s-1")).isEmpty();
    }

    @Test
    void getQueryCheckpointsReturnsSortedByTime() {
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-1", "s-2", 2, 200, List.of(), 200L));
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-1", "s-1", 4, 400, List.of(), 100L));
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-1", "s-3", 1, 50, List.of(), 300L));

        List<StageCheckpoint> result = checkpoint.getQueryCheckpoints("q-1");
        assertThat(result).hasSize(3);
        assertThat(result.get(0).stageId()).isEqualTo("s-1");
        assertThat(result.get(1).stageId()).isEqualTo("s-2");
        assertThat(result.get(2).stageId()).isEqualTo("s-3");
    }

    @Test
    void getQueryCheckpointsDoesNotReturnOtherQueries() {
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-1", "s-1", 4, 100, List.of(), 100L));
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-2", "s-1", 2, 50, List.of(), 200L));

        assertThat(checkpoint.getQueryCheckpoints("q-1")).hasSize(1);
        assertThat(checkpoint.getQueryCheckpoints("q-2")).hasSize(1);
        assertThat(checkpoint.getQueryCheckpoints("q-3")).isEmpty();
    }

    @Test
    void clearQueryCheckpointsRemovesOnlyTargetQuery() {
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-1", "s-1", 4, 100, List.of(), 100L));
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-1", "s-2", 2, 50, List.of(), 200L));
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-2", "s-1", 1, 25, List.of(), 300L));

        checkpoint.clearQueryCheckpoints("q-1");

        assertThat(checkpoint.getQueryCheckpoints("q-1")).isEmpty();
        assertThat(checkpoint.getQueryCheckpoints("q-2")).hasSize(1);
        assertThat(checkpoint.size()).isEqualTo(1);
    }

    @Test
    void canResumeStageReturnsTrueAfterCheckpoint() {
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-1", "s-1", 4, 100, List.of("h-1"), 100L));

        assertThat(checkpoint.canResumeStage("q-1", "s-1")).isTrue();
        assertThat(checkpoint.canResumeStage("q-1", "s-2")).isFalse();
    }

    @Test
    void checkpointOverwritesPreviousValue() {
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-1", "s-1", 4, 100, List.of("h-old"), 100L));
        checkpoint.checkpointStage(new StageCheckpoint(
                "q-1", "s-1", 8, 200, List.of("h-new"), 200L));

        var result = checkpoint.getStageCheckpoint("q-1", "s-1");
        assertThat(result).isPresent();
        assertThat(result.get().outputPartitions()).isEqualTo(8);
        assertThat(result.get().outputRows()).isEqualTo(200);
        assertThat(result.get().shuffleHandles()).containsExactly("h-new");
        assertThat(checkpoint.size()).isEqualTo(1);
    }

    @Test
    void checkpointRejectsNull() {
        assertThatThrownBy(() -> checkpoint.checkpointStage(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
