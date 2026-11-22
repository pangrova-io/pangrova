package org.pangrova.protocol.task;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.checksum.ProtocolChecksums;
import org.pangrova.protocol.v2.DirectExchangeInput;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.ExchangePartitionSource;
import org.pangrova.protocol.v2.ExchangeReadMode;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.ShufflePartitionPublication;
import org.pangrova.protocol.v2.ShuffleServiceAddress;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Fences and bounds for the immutable bindings used from HTTP admission through JNI. */
class CanonicalExchangeInputValidationTest {
    private static final long DEADLINE = 4_102_444_800_000L;

    @Test
    void directSourceRequiresValidEpochFenceTicketAndPort() {
        var source = directSource().build();
        CanonicalTaskRequests.validateForAdmission(request().addExchangeInputs(direct(source)).build());
        for (var invalid : java.util.List.of(source.toBuilder().clearPlanEpoch(),
                source.toBuilder().clearFencingToken(), source.toBuilder().setPort(65_536),
                source.toBuilder().setSourcePartitionId(-1), source.toBuilder().clearSignedTicket())) {
            assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(
                    request().addExchangeInputs(direct(invalid.build())).build()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exchange source");
        }
    }

    @Test
    void duplicateLogicalProducerCannotBindASecondAttempt() {
        var original = directSource().build();
        var retry = original.toBuilder().setSourceTaskId("retry").setAttemptId(1).setFencingToken(2).build();
        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(
                request().addExchangeInputs(direct(original, retry)).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicated");
    }

    @Test
    void externalEndpointOrderAndDeadlineArePartOfTheCanonicalBinding() {
        var task = request();
        var endpoint = ShuffleServiceEndpoint.newBuilder().setHost("shuffle").setPort(9090);
        var first = endpoint.setPartitionStart(0).setPartitionEnd(1).build();
        var second = endpoint.setPartitionStart(1).setPartitionEnd(2).build();
        var digest = ProtocolChecksums.sha256(new byte[] {1});
        var read = ExternalShuffleInput.newBuilder().setShuffleId("shuffle-id").setPartitionCount(2)
                .setExecutionOwner(task.getAttempt().getExecutionOwner()).setPlanEpoch(1).setDeadlineEpochMs(DEADLINE)
                .setExpectedProducerPartitions(1).setExpectedProducerSetChecksum(digest)
                .setCommitAddress(ShuffleServiceAddress.newBuilder().setHost("shuffle").setPort(9090))
                .setReadCapability(ShuffleAccessCapability.newBuilder().setToken(ByteString.copyFromUtf8("ticket"))
                        .setExpiresAtEpochMs(DEADLINE + 1))
                .setPublication(ShufflePartitionPublication.newBuilder().setLastSequence(-1)
                        .setTotalBytes(0).setTotalRows(0).setManifestSha256(digest.getDigest()))
                .addEndpoints(first).addEndpoints(second).build();
        assertThat(CanonicalTaskRequests.validExternalInput(read)).isTrue();
        assertThat(CanonicalTaskRequests.validExternalInput(read.toBuilder().clearEndpoints()
                .addEndpoints(second).addEndpoints(first).build())).isFalse();
        assertThat(CanonicalTaskRequests.validExternalInput(read.toBuilder().clearExpectedProducerSetChecksum()
                .build())).isFalse();
        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(task
                .addExchangeInputs(input(1).setExternal(read.toBuilder().setDeadlineEpochMs(DEADLINE + 1))).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("external shuffle input");
    }

    private static TaskRequest.Builder request() {
        return CanonicalTaskRequestTestFixture.builder("task", "query", TaskKind.TASK_KIND_READ, new byte[] {1});
    }

    private static ExchangeInputBinding direct(ExchangePartitionSource... sources) {
        return input(sources.length).setDirect(DirectExchangeInput.newBuilder()
                .addAllSources(java.util.List.of(sources))).build();
    }

    private static ExchangeInputBinding.Builder input(int count) {
        return ExchangeInputBinding.newBuilder().setPlanNodeId("node").setExchangeId("exchange")
                .setNoMoreSources(true).setExpectedSourcePartitions(count)
                .setReadMode(ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING);
    }

    private static ExchangePartitionSource.Builder directSource() {
        return ExchangePartitionSource.newBuilder().setSourceStageId(3).setSourcePartitionId(7)
                .setSourceTaskId("producer").setHost("worker").setPort(9090).setPlanEpoch(1).setFencingToken(1)
                .setSignedTicket(ByteString.copyFromUtf8("ticket")).setTicketExpiresAtEpochMs(DEADLINE);
    }
}
