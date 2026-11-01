package org.pangrova.worker.property.exchange;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.protobuf.ByteString;

import java.util.List;
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
import org.pangrova.shuffle.api.exchange.DirectExchangeTransport;
import org.pangrova.worker.exchange.source.ExchangeSourceConverter;

/** Properties for canonical task-scoped exchange source conversion. */
@Tag("Feature: architecture-remediation")
class ExchangeSourceUpdatePropertyTest {

    @Property(tries = 50)
    void conversionPreservesEveryLogicalProducerExactlyOnce(
            @ForAll("sourceCounts") int count,
            @ForAll("planEpochs") long planEpoch) {
        var inputs = ExchangeSourceConverter.toExchangeInputs(
                List.of(binding(count, planEpoch, 0)), attempt(), deadline());

        assertThat(inputs).hasSize(1);
        assertThat(inputs.get(0).expectedSourcePartitions()).isEqualTo(count);
        var sources = ((DirectExchangeTransport) inputs.get(0).transport()).sources();
        assertThat(sources).hasSize(count);
        assertThat(sources)
                .extracting(source -> source.sourceStageId() + "#" + source.sourcePartitionId())
                .doesNotHaveDuplicates();
        assertThat(sources)
                .allSatisfy(source -> {
                    assertThat(source.planEpoch()).isEqualTo(planEpoch);
                    assertThat(source.fencingToken()).isEqualTo(fence(planEpoch, 0));
                    assertThat(source.signedTicket()).containsExactly(1, 2, 3);
                });
    }

    @Property(tries = 25)
    void newerAttemptCarriesStrictlyGreaterFence(
            @ForAll("planEpochs") long planEpoch,
            @ForAll("attempts") int attempt) {
        var oldSource = ExchangeSourceConverter.toExchangeInputs(
                List.of(binding(1, planEpoch, attempt)), attempt(), deadline());
        var retry = ExchangeSourceConverter.toExchangeInputs(
                List.of(binding(1, planEpoch, attempt + 1)), attempt(), deadline());
        var oldEndpoint = ((DirectExchangeTransport)
                oldSource.get(0).transport()).sources().get(0);
        var retryEndpoint = ((DirectExchangeTransport)
                retry.get(0).transport()).sources().get(0);

        assertThat(retryEndpoint.sourceStageId()).isEqualTo(oldEndpoint.sourceStageId());
        assertThat(retryEndpoint.sourcePartitionId()).isEqualTo(oldEndpoint.sourcePartitionId());
        assertThat(retryEndpoint.attemptId()).isGreaterThan(oldEndpoint.attemptId());
        assertThat(retryEndpoint.fencingToken()).isGreaterThan(oldEndpoint.fencingToken());
    }

    @Property(tries = 10)
    void nullAndEmptyInputsProduceNoBindings(
            @ForAll("emptyInputs") List<ExchangeInputBinding> input) {
        assertThat(ExchangeSourceConverter.toExchangeInputs(
                input, attempt(), deadline())).isEmpty();
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

    private static long deadline() {
        return 4_102_444_800_000L;
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

    @Provide
    Arbitrary<List<ExchangeInputBinding>> emptyInputs() {
        return Arbitraries.of(null, List.of());
    }
}
