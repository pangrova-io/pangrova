package org.pangrova.worker.shuffle;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.ExchangeReadMode;
import org.pangrova.protocol.v2.ShufflePersistenceMode;
import org.pangrova.protocol.v2.TaskRequest;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalShuffleTaskBindingTest {
    @Test
    void rejectsForeignOrUnregisteredSourcesAndRetriesBeforeCallingNative() {
        var local = new NativeShuffleProducerRegistration((h, c) -> { throw new AssertionError(); }, "worker", "", true);
        var producer = TaskRequest.newBuilder();
        producer.getExecutionOptionsBuilder().getShuffleBuilder()
                .setPersistenceMode(ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_DISK);
        assertThatCode(() -> local.validateTask(producer.build())).doesNotThrowAnyException();
        producer.getAttemptBuilder().setAttemptId(1);
        assertThatThrownBy(() -> local.validateTask(producer.build())).hasMessageContaining("replacement attempt");
        producer.getAttemptBuilder().clearAttemptId();
        producer.getExecutionOptionsBuilder().getShuffleBuilder()
                .setPersistenceMode(ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MODE_MEMORY_ONLY);
        assertThatThrownBy(() -> local.validateTask(producer.build())).hasMessageContaining("Worker deployment");
        var input = ExchangeInputBinding.newBuilder().setReadMode(ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING)
                .setNoMoreSources(true);
        input.getDirectBuilder().addSourcesBuilder().setHost("another").setPort(1);
        assertThatThrownBy(() -> local.validateTask(TaskRequest.newBuilder().addExchangeInputs(input).build()))
                .hasMessageContaining("not registered on this Worker");
        input.setReadMode(ExchangeReadMode.EXCHANGE_READ_MODE_STREAMING);
        assertThatThrownBy(() -> local.validateTask(TaskRequest.newBuilder().addExchangeInputs(input).build()))
                .hasMessageContaining("blocking producer");
    }
}
