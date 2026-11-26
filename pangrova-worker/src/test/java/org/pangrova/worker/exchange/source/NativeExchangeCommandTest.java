package org.pangrova.worker.exchange.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.DirectExchangeInput;
import org.pangrova.protocol.v2.ExchangePartitionSource;
import org.pangrova.protocol.v2.ExchangeReadMode;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.ConnectExchangeSourceRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.worker.jni.NativeCommandBuilder;

/** Verifies the actual JNI command retains each admitted source without reconstructing it. */
class NativeExchangeCommandTest {

    @Test
    void preservesIdentityFenceTicketAndExpiry() throws Exception {
        var source = source("task-1", 0, 1, 0);
        var input = binding(1, source);
        var consumer = attempt();
        var command = RuntimeCommand.parseFrom(NativeCommandBuilder.connectDirectExchange(consumer, input, source));
        var payload = command.getPayload().unpack(ConnectExchangeSourceRequest.class);
        assertThat(command.getAttempt()).isEqualTo(consumer);
        assertThat(payload.getPlanNodeId()).isEqualTo(input.getPlanNodeId());
        assertThat(payload.getExchangeId()).isEqualTo(input.getExchangeId());
        assertThat(payload.getDirect()).isEqualTo(source);
    }

    @Test
    void preservesCanonicalInputBoundaries() throws Exception {
        var first = binding(2, source("task-1", 0, 1, 0), source("task-2", 1, 1, 0));
        var second = ExchangeInputBinding.newBuilder()
                .setPlanNodeId("other-node").setExchangeId("exchange-2")
                .setExpectedSourcePartitions(1).setNoMoreSources(true)
                .setReadMode(ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING)
                .setDirect(DirectExchangeInput.newBuilder()
                        .addSources(source("task-3", 0, 1, 0))).build();
        for (var input : List.of(first, second)) {
            for (var source : input.getDirect().getSourcesList()) {
                var wire = NativeCommandBuilder.connectDirectExchange(attempt(), input, source);
                var payload = RuntimeCommand.parseFrom(wire).getPayload().unpack(ConnectExchangeSourceRequest.class);
                assertThat(payload.getPlanNodeId()).isEqualTo(input.getPlanNodeId());
                assertThat(payload.getExchangeId()).isEqualTo(input.getExchangeId());
                assertThat(payload.getDirect()).isEqualTo(source);
            }
        }
    }

    private static ExchangeInputBinding binding(
            int expectedSources, ExchangePartitionSource... sources) {
        return ExchangeInputBinding.newBuilder()
                .setPlanNodeId("exchange-node").setExchangeId("exchange-1")
                .setExpectedSourcePartitions(expectedSources).setNoMoreSources(true)
                .setReadMode(ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING)
                .setDirect(DirectExchangeInput.newBuilder()
                        .addAllSources(List.of(sources))).build();
    }

    private static ExchangePartitionSource source(
            String taskId, int sourcePartitionId, long planEpoch, int attemptId) {
        return ExchangePartitionSource.newBuilder()
                .setSourceTaskId(taskId).setSourceStageId(1)
                .setSourcePartitionId(sourcePartitionId)
                .setOutputPartitionId(sourcePartitionId)
                .setPlanEpoch(planEpoch).setAttemptId(attemptId)
                .setFencingToken(fence(planEpoch, attemptId))
                .setHost("host-" + sourcePartitionId)
                .setPort(9_090 + sourcePartitionId)
                .setSignedTicket(ByteString.copyFrom(new byte[]{1, 2, 3}))
                .setTicketExpiresAtEpochMs(4_102_444_800_000L)
                .setExpectedRows(10).setExpectedBytes(80).build();
    }

    private static long fence(long epoch, int attempt) {
        return (epoch << 32) + attempt + 1L;
    }

    private static TaskAttemptId attempt() {
        return TaskAttemptId.newBuilder()
                .setQueryOrJobId("query-1").setStageId(2).setPartitionId(3)
                .setPlanEpoch(1).setAttemptId(0).setFencingToken(fence(1, 0))
                .setTaskId("consumer-2-3").setControlEpoch(7)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("coordinator-1")
                        .setOwnerIncarnation("incarnation-1")
                        .setFencingToken(9))
                .build();
    }
}
