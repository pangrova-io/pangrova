package org.pangrova.worker.execution;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link NativeTaskAdmissionController}.
 * Task 200: validates fan-out deadlock prevention via admission control.
 */
class NativeTaskAdmissionControllerTest {

    @Test
    void acquireAndReleaseSlots() throws InterruptedException {
        var controller = new NativeTaskAdmissionController(3);
        assertThat(controller.availableSlots()).isEqualTo(3);
        assertThat(controller.activeTaskCount()).isZero();

        assertThat(controller.acquire(100)).isTrue();
        assertThat(controller.availableSlots()).isEqualTo(2);
        assertThat(controller.activeTaskCount()).isEqualTo(1);

        controller.release();
        assertThat(controller.availableSlots()).isEqualTo(3);
    }

    @Test
    void acquireTimesOutWhenFull() throws InterruptedException {
        var controller = new NativeTaskAdmissionController(1);
        assertThat(controller.acquire(100)).isTrue();
        // All slots taken
        assertThat(controller.acquire(50)).isFalse();
        controller.release();
    }

    @Test
    void rejectsNonPositiveMax() {
        assertThatThrownBy(() -> new NativeTaskAdmissionController(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maxConcurrentTasksAccessor() {
        var controller = new NativeTaskAdmissionController(4);
        assertThat(controller.maxConcurrentTasks()).isEqualTo(4);
    }

    @Test
    void concurrentAcquisitionRespectsBound() throws InterruptedException {
        int max = 3;
        var controller = new NativeTaskAdmissionController(max);

        for (int i = 0; i < max; i++) {
            assertThat(controller.acquire(100)).isTrue();
        }
        assertThat(controller.availableSlots()).isZero();
        assertThat(controller.activeTaskCount()).isEqualTo(max);

        // Should time out
        assertThat(controller.acquire(10)).isFalse();

        // Release one
        controller.release();
        assertThat(controller.acquire(100)).isTrue();
    }
}
