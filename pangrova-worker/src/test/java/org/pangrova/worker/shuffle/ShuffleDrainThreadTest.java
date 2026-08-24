package org.pangrova.worker.shuffle;

import org.pangrova.worker.execution.SimulatedTaskExecutor;
import org.pangrova.worker.shuffle.ShuffleDrainThread.ShuffleSender;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShuffleDrainThreadTest {

    private final SimulatedTaskExecutor executor = new SimulatedTaskExecutor(0);

    @Test
    void rejectsZeroExecutorHandle() {
        assertThatThrownBy(() -> new ShuffleDrainThread(0, noOpSender(), executor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("executorHandle");
    }

    @Test
    void rejectsNullSender() {
        assertThatThrownBy(() -> new ShuffleDrainThread(1L, null, executor))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("sender");
    }

    @Test
    void rejectsNullTaskExecutor() {
        assertThatThrownBy(() -> new ShuffleDrainThread(1L, noOpSender(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("taskExecutor");
    }

    @Test
    void constructsWithValidArguments() {
        ShuffleDrainThread thread = new ShuffleDrainThread(1L, noOpSender(), executor);
        assertThat(thread).isNotNull();
    }

    private static ShuffleSender noOpSender() {
        return data -> {};
    }
}
