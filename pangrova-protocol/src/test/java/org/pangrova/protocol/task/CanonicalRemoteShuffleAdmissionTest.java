package org.pangrova.protocol.task;

import org.pangrova.protocol.CanonicalTaskRequestTestFixture;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.protocol.v2.ShufflePartitionPublication;
import com.google.protobuf.ByteString;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.protocol.v2.ShuffleExecutionBinding;
import org.pangrova.protocol.v2.ShufflePersistenceMode;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.protocol.v2.ShuffleTaskOptions;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.junit.jupiter.api.Test;
import java.util.List;

class CanonicalRemoteShuffleAdmissionTest {

    @Test
    void partitionPublicationRequiresPresentBoundsAndFullDigestEvenForEmptyOutput() {
        var empty = ShufflePartitionPublication.newBuilder()
                .setLastSequence(-1).setTotalRows(0).setTotalBytes(0)
                .setManifestSha256(ByteString.copyFrom(new byte[32])).build();
        var populated = empty.toBuilder().setLastSequence(1).setTotalRows(2).setTotalBytes(3).build();
        assertThat(CanonicalTaskRequests.validPartitionPublication(empty)).isTrue();
        assertThat(CanonicalTaskRequests.validPartitionPublication(populated)).isTrue();
        for (var invalid : List.of(
                empty.toBuilder().clearLastSequence(), empty.toBuilder().clearTotalRows(),
                empty.toBuilder().clearTotalBytes(), empty.toBuilder().setTotalBytes(1),
                empty.toBuilder().setLastSequence(-2), populated.toBuilder().setLastSequence(2),
                populated.toBuilder().clearManifestSha256(),
                populated.toBuilder().setManifestSha256(ByteString.copyFrom(new byte[33])))) {
            assertThat(CanonicalTaskRequests.validPartitionPublication(invalid.build())).isFalse();
        }
    }

    @Test
    void rejectsUnspecifiedRemovedAndUnknownWireFormats() {
        for (int value : List.of(0, 1, 3, -1, Integer.MAX_VALUE)) {
            var request = requestBuilder();
            var options = request.getExecutionOptions();
            request.setExecutionOptions(options.toBuilder().setShuffle(
                    options.getShuffle().toBuilder().setSerdeValue(value)));
            assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request.build()))
                    .as("wire format %s", value).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("shuffle execution binding");
        }
    }

    @Test
    void onlyExactCanonicalCompressionNamesAreAdmitted() {
        for (String value : List.of("none", "lz4", "zstd")) {
            var request = requestBuilder();
            var options = request.getExecutionOptions();
            request.setExecutionOptions(options.toBuilder().setShuffle(
                    options.getShuffle().toBuilder().setCompressionCodec(value)));
            assertThatCode(() -> CanonicalTaskRequests.validateForAdmission(request.build()))
                    .as("codec %s", value).doesNotThrowAnyException();
        }
        for (String value : List.of("", "NONE", "LZ4", "ZSTD", "gzip", " none")) {
            var request = requestBuilder();
            var options = request.getExecutionOptions();
            request.setExecutionOptions(options.toBuilder().setShuffle(
                    options.getShuffle().toBuilder().setCompressionCodec(value)));
            assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request.build()))
                    .as("codec %s", value).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("shuffle execution binding");
        }
    }

    @Test
    void batchRowsMustFitPositiveSignedExecutionDomain() {
        for (int value : List.of(0, -1, Integer.MIN_VALUE)) {
            var request = requestBuilder();
            var options = request.getExecutionOptions();
            request.setExecutionOptions(options.toBuilder().setShuffle(
                    options.getShuffle().toBuilder().setBatchSize(value)));
            assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request.build()))
                    .as("batch rows %s", value).isInstanceOf(IllegalArgumentException.class);
        }
        var request = requestBuilder();
        var options = request.getExecutionOptions();
        request.setExecutionOptions(options.toBuilder().setShuffle(
                options.getShuffle().toBuilder().setBatchSize(Integer.MAX_VALUE)));
        assertThatCode(() -> CanonicalTaskRequests.validateForAdmission(request.build())).doesNotThrowAnyException();
    }

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
        request.getResourceGrantBuilder().setShuffleBufferBytes(65_536);
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
