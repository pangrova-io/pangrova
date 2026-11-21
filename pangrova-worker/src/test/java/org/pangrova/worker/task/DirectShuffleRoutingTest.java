package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ShuffleExecutionBinding;
import org.pangrova.protocol.v2.ShuffleTaskOptions;

import static org.assertj.core.api.Assertions.assertThat;

class DirectShuffleRoutingTest {

    @Test
    void unboundShuffleUsesNativeFlightDemuxer() {
        var shuffle = ShuffleTaskOptions.newBuilder()
                .setPartitionCount(2)
                .build();
        assertThat(TaskLifecycleManager.isDirectFlightShuffle(shuffle)).isTrue();
    }

    @Test
    void remoteShuffleBindingMustNotCreateCompetingFlightConsumer() {
        var shuffle = ShuffleTaskOptions.newBuilder()
                .setPartitionCount(2)
                .setExecutionBinding(ShuffleExecutionBinding.newBuilder().build())
                .build();
        assertThat(TaskLifecycleManager.isDirectFlightShuffle(shuffle)).isFalse();
    }
}
