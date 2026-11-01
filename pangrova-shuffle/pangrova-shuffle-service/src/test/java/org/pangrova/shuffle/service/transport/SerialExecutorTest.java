package org.pangrova.shuffle.service.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SerialExecutorTest {

    @Test
    void reportsRejectedInitialAdmission() {
        SerialExecutor serial = new SerialExecutor(command -> {
            throw new RejectedExecutionException("full");
        }, failure -> {});

        assertThat(serial.execute(() -> {})).isFalse();
    }

    @Test
    void drainsTasksInSubmissionOrder() {
        List<Integer> order = new ArrayList<>();
        SerialExecutor serial = new SerialExecutor(Runnable::run, failure -> {});

        assertThat(serial.execute(() -> order.add(1))).isTrue();
        assertThat(serial.execute(() -> order.add(2))).isTrue();

        assertThat(order).containsExactly(1, 2);
    }

    @Test
    void reportsTaskFailureAndStopsDrain() {
        AtomicReference<RuntimeException> observed = new AtomicReference<>();
        SerialExecutor serial = new SerialExecutor(Runnable::run, observed::set);
        IllegalStateException failure = new IllegalStateException("failed");

        assertThat(serial.execute(() -> {
            throw failure;
        })).isTrue();

        assertThat(observed).hasValue(failure);
    }
}
