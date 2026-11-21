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
import org.pangrova.shuffle.exchange.DirectExchangeTransport;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;

/** Verifies canonical v2 exchange bindings become task-scoped inputs exactly once. */
class ExchangeSourceConverterTest {

    @Test
    void preservesIdentityFenceTicketAndExpiry() {
        var inputs = ExchangeSourceConverter.toExchangeInputs(
                List.of(binding(1, source("task-1", 0, 1, 0))),
                attempt(), deadline());

        assertThat(inputs).hasSize(1);
        assertThat(inputs.get(0).planNodeId()).isEqualTo("exchange-node");
        assertThat(inputs.get(0).exchangeId()).isEqualTo("exchange-1");
        assertThat(inputs.get(0).expectedSourcePartitions()).isEqualTo(1);
        ExchangeEndpoint endpoint = ((DirectExchangeTransport)
                inputs.get(0).transport()).sources().get(0);
        assertThat(endpoint.sourceTaskId()).isEqualTo("task-1");
        assertThat(endpoint.sourceStageId()).isEqualTo(1);
        assertThat(endpoint.sourcePartitionId()).isZero();
        assertThat(endpoint.outputPartitionId()).isZero();
        assertThat(endpoint.planEpoch()).isEqualTo(1);
        assertThat(endpoint.attemptId()).isZero();
        assertThat(endpoint.fencingToken()).isEqualTo(fence(1, 0));
        assertThat(endpoint.signedTicket()).containsExactly(1, 2, 3);
        assertThat(endpoint.ticketExpiresAtEpochMs()).isEqualTo(4_102_444_800_000L);
    }

    @Test
    void preservesCanonicalInputBoundaries() {
        var first = binding(2,
                source("task-1", 0, 1, 0), source("task-2", 1, 1, 0));
        var second = ExchangeInputBinding.newBuilder()
                .setPlanNodeId("other-node").setExchangeId("exchange-2")
                .setExpectedSourcePartitions(1).setNoMoreSources(true)
                .setReadMode(ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING)
                .setDirect(DirectExchangeInput.newBuilder()
                        .addSources(source("task-3", 0, 1, 0))).build();

        var inputs = ExchangeSourceConverter.toExchangeInputs(
                List.of(first, second), attempt(), deadline());

        assertThat(inputs).hasSize(2);
        assertThat(((DirectExchangeTransport) inputs.get(0).transport()).sources())
                .hasSize(2);
        assertThat(((DirectExchangeTransport) inputs.get(1).transport()).sources())
                .hasSize(1);
    }

    @Test
    void returnsEmptyForNullOrEmptyBindings() {
        assertThat(ExchangeSourceConverter.toExchangeInputs(
                null, attempt(), deadline())).isEmpty();
        assertThat(ExchangeSourceConverter.toExchangeInputs(
                List.of(), attempt(), deadline())).isEmpty();
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

    private static long deadline() {
        return 4_102_444_800_000L;
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
