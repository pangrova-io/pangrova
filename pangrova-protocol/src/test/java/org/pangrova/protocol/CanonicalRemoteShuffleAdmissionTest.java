package org.pangrova.protocol;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.protocol.v2.ShuffleExecutionBinding;
import org.pangrova.protocol.v2.ShufflePersistenceMode;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.protocol.v2.ShuffleTaskOptions;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.junit.jupiter.api.Test;

class CanonicalRemoteShuffleAdmissionTest {

    @Test
    void acceptsOwnerAndEpochBoundRemoteShuffle() {
        var request = requestBuilder().build();

        assertThatCode(() -> CanonicalTaskRequests.validateForAdmission(request))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsRemoteShuffleWithoutAllocation() {
        var request = requestBuilder()
                .setExecutionOptions(TaskExecutionOptions.newBuilder()
                        .setShuffle(shuffle().clearExecutionBinding()))
                .build();

        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shuffle execution binding");
    }

    @Test
    void rejectsOwnerOrPlanEpochDrift() {
        var request = requestBuilder()
                .setExecutionOptions(TaskExecutionOptions.newBuilder()
                        .setShuffle(shuffle().setExecutionBinding(
                                binding().setPlanEpoch(2L))))
                .build();

        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shuffle execution binding");
    }

    private static org.pangrova.protocol.v2.TaskRequest.Builder requestBuilder() {
        byte[] plan = new byte[] {1};
        var request = CanonicalTaskRequestTestFixture.builder(
                "task-1", "query-1", TaskKind.TASK_KIND_SHUFFLE_WRITE, plan);
        return request.setExecutionOptions(TaskExecutionOptions.newBuilder()
                .setShuffle(shuffle().setExecutionBinding(binding()
                        .setExecutionOwner(request.getAttempt().getExecutionOwner()))));
    }

    private static ShuffleTaskOptions.Builder shuffle() {
        return ShuffleTaskOptions.newBuilder()
                .setSerde(org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH)
                .setCompressionCodec("zstd")
                .setBatchSize(1024)
                .setPartitionCount(8)
                .setPersistenceMode(ShufflePersistenceMode
                        .SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE);
    }

    private static ShuffleExecutionBinding.Builder binding() {
        return ShuffleExecutionBinding.newBuilder()
                .setShuffleId("shuffle-1")
                .setPlanEpoch(1L)
                .addEndpoints(ShuffleServiceEndpoint.newBuilder()
                        .setHost("rss-1")
                        .setPort(9400)
                        .setTlsIdentity("rss-1"));
    }
}
