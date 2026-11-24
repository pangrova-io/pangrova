package org.pangrova.worker.property.exchange;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.protobuf.ByteString;

import java.util.stream.IntStream;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.DirectExchangeInput;
import org.pangrova.protocol.v2.ExchangePartitionSource;
import org.pangrova.protocol.v2.ExchangeReadMode;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.ConnectExchangeSourceRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.worker.jni.NativeCommandBuilder;

/** Properties for direct source identities in actual JNI commands. */
@Tag("Feature: architecture-remediation")
class ExchangeSourceUpdatePropertyTest {

    @Property(tries = 50)
    void commandPreservesEveryLogicalProducerExactlyOnce(
            @ForAll("sourceCounts") int count,
            @ForAll("planEpochs") long planEpoch) throws Exception {
        var input = binding(count, planEpoch, 0);
        var sources = new java.util.ArrayList<ExchangePartitionSource>();
        for (var source : input.getDirect().getSourcesList()) {
            sources.add(delivered(input, source));
        }
        assertThat(sources).containsExactlyElementsOf(input.getDirect().getSourcesList());
        assertThat(sources).hasSize(count).extracting(source -> source.getSourceStageId() + "#"
                + source.getSourcePartitionId()).doesNotHaveDuplicates();
    }

    @Property(tries = 25)
    void newerAttemptCarriesStrictlyGreaterFence(
            @ForAll("planEpochs") long planEpoch,
            @ForAll("attempts") int attempt) throws Exception {
        var previous = binding(1, planEpoch, attempt);
        var replacement = binding(1, planEpoch, attempt + 1);
        var oldEndpoint = delivered(previous, previous.getDirect().getSources(0));
        var retryEndpoint = delivered(replacement, replacement.getDirect().getSources(0));
        assertThat(retryEndpoint.getSourceStageId()).isEqualTo(oldEndpoint.getSourceStageId());
        assertThat(retryEndpoint.getSourcePartitionId()).isEqualTo(oldEndpoint.getSourcePartitionId());
        assertThat(retryEndpoint.getAttemptId()).isGreaterThan(oldEndpoint.getAttemptId());
        assertThat(retryEndpoint.getFencingToken()).isGreaterThan(oldEndpoint.getFencingToken());
    }

    private static ExchangePartitionSource delivered(ExchangeInputBinding input, ExchangePartitionSource source)
            throws Exception {
        var consumer = attempt();
        var command = RuntimeCommand.parseFrom(NativeCommandBuilder.connectDirectExchange(consumer, input, source));
        assertThat(command.getAttempt()).isEqualTo(consumer);
        return command.getPayload().unpack(ConnectExchangeSourceRequest.class).getDirect();
    }

    private static ExchangeInputBinding binding(
            int expectedSources, long planEpoch, int attempt) {
        var sources = DirectExchangeInput.newBuilder();
        var binding = ExchangeInputBinding.newBuilder()
                .setPlanNodeId("exchange-node").setExchangeId("exchange-1")
                .setExpectedSourcePartitions(expectedSources).setNoMoreSources(true)
                .setReadMode(ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING);
        IntStream.range(0, expectedSources).forEach(sourcePartition ->
                sources.addSources(ExchangePartitionSource.newBuilder()
                        .setSourceTaskId("task-" + sourcePartition + "-attempt-" + attempt)
                        .setSourceStageId(7).setSourcePartitionId(sourcePartition)
                        .setOutputPartitionId(sourcePartition)
                        .setPlanEpoch(planEpoch).setAttemptId(attempt)
                        .setFencingToken(fence(planEpoch, attempt))
                        .setHost("host-" + sourcePartition).setPort(9_000 + sourcePartition)
                        .setSignedTicket(ByteString.copyFrom(new byte[]{1, 2, 3}))
                        .setTicketExpiresAtEpochMs(4_102_444_800_000L)));
        return binding.setDirect(sources).build();
    }

    private static long fence(long planEpoch, int attempt) {
        return (planEpoch << 32) + attempt + 1L;
    }

    private static TaskAttemptId attempt() {
        return TaskAttemptId.newBuilder()
                .setQueryOrJobId("query-1").setStageId(8).setPartitionId(0)
                .setPlanEpoch(1).setAttemptId(0).setFencingToken(fence(1, 0))
                .setTaskId("consumer-8-0").setControlEpoch(3)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("coordinator-1")
                        .setOwnerIncarnation("incarnation-1")
                        .setFencingToken(5))
                .build();
    }

    @Provide
    Arbitrary<Integer> sourceCounts() {
        return Arbitraries.integers().between(1, 10);
    }

    @Provide
    Arbitrary<Long> planEpochs() {
        return Arbitraries.longs().between(1, 1_000_000);
    }

    @Provide
    Arbitrary<Integer> attempts() {
        return Arbitraries.integers().between(0, 1_000);
    }

}
