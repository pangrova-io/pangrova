package org.pangrova.worker.exchange;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.shuffle.exchange.DirectExchangeTransport;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.pangrova.shuffle.exchange.ExchangeInput;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;
import org.pangrova.testing.nativefixture.ActualNativeShuffleRequests;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.nativebridge.shuffle.NativeShuffleTransport;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Native endpoint registration and exact mapper metadata; not distributed execution proof. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class NativeExchangeTransportTest {
    @Test
    void bindInputsSendsEveryDirectSourceThenSealsWithAttempt() {
        try (var runtime = new ActualNativeTaskFixture()) {
            var producer = ActualNativeShuffleRequests.pendingProducer(1);
            long first = runtime.createPending(producer);
            var secondRequest = producer.toBuilder().setAttempt(producer.getAttempt().toBuilder()
                    .setTaskId(producer.getAttempt().getTaskId() + "-second").setPartitionId(1)).build();
            long second = runtime.createPending(secondRequest);
            var servers = new NativeShuffleTransport(runtime.executor()::executeCommand, "127.0.0.1", "");
            var firstServer = servers.startServer(first, 1);
            var secondServer = servers.startServer(second, 1);
            var consumer = runtime.valuesRequest(false);
            long handle = runtime.createPending(consumer);
            var firstSource = endpoint(producer, firstServer);
            var secondSource = endpoint(secondRequest, secondServer);
            var input = new ExchangeInput("exchange-node", "exchange-1", 2,
                    new DirectExchangeTransport(List.of(firstSource, secondSource)));
            runtime.transports().exchangeTransport().bindInputs(handle, attempt(consumer), List.of(input));
            var source = ExchangeProtocolMapper.directSource(firstSource);
            assertThat(source.getSourceTaskId()).isEqualTo(producer.getAttempt().getTaskId());
            assertThat(source.getSourceStageId()).isEqualTo(producer.getAttempt().getStageId());
            assertThat(source.getSourcePartitionId()).isZero();
            assertThat(source.getOutputPartitionId()).isZero();
            assertThat(source.getPlanEpoch()).isEqualTo(producer.getAttempt().getPlanEpoch());
            assertThat(source.getFencingToken()).isEqualTo(producer.getAttempt().getFencingToken());
            assertThat(source.getSignedTicket().toByteArray()).isEqualTo(firstServer.partitionTickets().get(0).signedTicket());
            assertThat(source.getTicketExpiresAtEpochMs()).isEqualTo(firstServer.partitionTickets().get(0).expiresAtEpochMs());
            assertThat(ExchangeProtocolMapper.taskAttempt(attempt(consumer))).isEqualTo(consumer.getAttempt());
            assertThat(runtime.executor().releaseTask(handle)).isPresent();
            assertThatThrownBy(() -> runtime.transports().exchangeTransport()
                    .bindInputs(handle, attempt(consumer), List.of(input))).isInstanceOf(IllegalStateException.class);
            servers.stopServer(first);
            servers.stopServer(second);
        }
    }

    @Test
    void bindInputsRejectsNonPositiveHandle() {
        try (var runtime = new ActualNativeTaskFixture()) {
            var attempt = attempt(runtime.valuesRequest(false));
            for (long handle : new long[]{0, -1}) {
                assertThatThrownBy(() -> runtime.transports().exchangeTransport().bindInputs(handle, attempt, List.of()))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("executorHandle must be positive");
            }
        }
    }

    @Test
    void bindInputsRejectsMissingAttemptOrInputs() {
        try (var runtime = new ActualNativeTaskFixture()) {
            var request = runtime.valuesRequest(false);
            long handle = runtime.createPending(request);
            assertThatThrownBy(() -> runtime.transports().exchangeTransport().bindInputs(handle, null, List.of()))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("consumerAttempt must not be null");
            assertThatThrownBy(() -> runtime.transports().exchangeTransport().bindInputs(handle, attempt(request), null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("inputs must not be null");
        }
    }

    @Test
    void bindInputsAcceptsNoInputsWithoutIssuingCommands() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var request = runtime.valuesRequest(false);
            long handle = runtime.createPending(request);
            var before = runtime.executor().getTaskStatus(handle);
            runtime.transports().exchangeTransport().bindInputs(handle, attempt(request), List.of());
            assertThat(runtime.executor().getTaskStatus(handle)).isEqualTo(before);
        }
    }

    @Test
    void constructorRejectsNullExecutor() {
        assertThatThrownBy(() -> new NativeExchangeTransport(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("taskExecutor must not be null");
    }

    static ExchangeTaskAttempt attempt(TaskRequest request) {
        return org.pangrova.worker.exchange.source.ExchangeSourceConverter
                .toExchangeTaskAttempt(request.getAttempt());
    }

    private static ExchangeEndpoint endpoint(TaskRequest request, org.pangrova.shuffle.api.ShuffleServerHandle server) {
        var value = request.getAttempt();
        var ticket = server.partitionTickets().get(0);
        return new ExchangeEndpoint(value.getTaskId(), value.getStageId(), value.getPartitionId(), ticket.partitionId(),
                value.getPlanEpoch(), value.getAttemptId(), value.getFencingToken(), server.host(), server.port(),
                server.tlsIdentity(), ticket.signedTicket(), ticket.expiresAtEpochMs(), 0, 0);
    }
}
