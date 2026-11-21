package org.pangrova.testing.integration.e2e.shuffle;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.protocol.v2.shuffle.CommitProducerAttemptRequest;
import org.pangrova.protocol.v2.shuffle.WriteBatchRequest;
import org.pangrova.shuffle.exchange.ExchangeInput;
import org.pangrova.shuffle.api.worker.ShuffleByteEgress;
import org.pangrova.testing.nativefixture.ActualNativeResultReader;
import org.pangrova.testing.nativefixture.ActualNativeShuffleRequests;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.jni.NativeRuntime;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real C++ producer → SPI callback → durable mTLS service → C++ consumer → Flight result. */
@Tag("native-required")
@Tag("native-runtime-owner")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
@ResourceLock(ActualShuffleBackend.CREDENTIAL_LOCK)
class NativeShuffleEgressContractIntegrationTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test void nativeCallbacksReachTheProductionContractAndPublishReadableBytes() throws Exception {
        try (var service = new ActualShuffleService(directory, 1, 1);
             var runtime = new ActualNativeTaskFixture();
             var worker = new org.pangrova.worker.bootstrap.ownership.WorkerTaskOwnership(service.workerShuffle());
             var egress = worker.shuffle().producers();
             var producerExecutor = new NativeTaskExecutor(egress);
             var consumerTransport = new NativeExchangeTransport(runtime.executor(), worker.shuffle().readers())) {
            ShuffleByteEgress callbacks = egress;
            NativeRuntime.nativeRegisterEgress(callbacks);
            var producer = service.registerProducer(base -> runtime.bindWorker(withActualExchangeSink(base)));
            var request = producer.request();
            long producerHandle = producerExecutor.createTask(request.toByteArray());
            producerExecutor.startTask(producerHandle);
            await().atMost(Duration.ofSeconds(20)).until(() -> producerExecutor.isFinished(producerHandle));
            var produced = TaskReport.parseFrom(producerExecutor.getTaskStatus(producerHandle));
            assertThat(produced.getAttempt()).isEqualTo(request.getAttempt());
            assertThat(produced.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            assertThat(produced.hasError()).isFalse();
            assertThat(service.wire(0).requests.stream().map(ShuffleWireRelay.Frame::message)
                    .filter(WriteBatchRequest.class::isInstance).map(WriteBatchRequest.class::cast).toList())
                    .singleElement().satisfies(batch -> {
                        assertThat(batch.getRowCount()).isEqualTo(1);
                        assertThat(batch.getChunk().getBatchSequence()).isZero();
                        assertThat(batch.getChunk().getData()).isNotEmpty();
                    });
            assertThat(service.wire(0).requests.stream().map(ShuffleWireRelay.Frame::message)
                    .filter(CommitProducerAttemptRequest.class::isInstance).toList()).hasSize(1);

            service.finalizeProducer(producer);
            var consumer = runtime.bindWorker(ActualExternalShuffleTask.consumer(producer));
            var binding = service.delegateRead(producer, consumer);
            long consumerHandle = runtime.createPending(consumer);
            consumerTransport.bindInputs(consumerHandle, binding.consumerAttempt(), List.of(
                    new ExchangeInput(ActualExternalShuffleTask.PLAN_NODE,
                            ActualExternalShuffleTask.EXCHANGE_ID, 1, binding)));
            runtime.executor().startTask(consumerHandle);
            var values = ActualNativeResultReader.count(runtime, consumer);
            assertThat(values.rows()).isEqualTo(1);
            assertThat(values.sum()).isEqualTo(42);
            assertThat(runtime.awaitFinished(consumerHandle).getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            consumerTransport.releaseInputs(consumerHandle);
            runtime.executor().releaseTask(consumerHandle).orElseThrow();
            producerExecutor.releaseTask(producerHandle).orElseThrow();
            assertThat(producerExecutor.releaseTask(producerHandle)).isEmpty();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
            assertThat(runtime.arbitrator().getUsedMemory()).isZero();
        }
    }

    private static TaskRequest withActualExchangeSink(TaskRequest base) {
        try {
            var actual = ActualNativeShuffleRequests.producer(1, base.getSubstraitPlan().toByteArray());
            return base.toBuilder().setSubstraitPlan(actual.getSubstraitPlan())
                    .setSubstraitPlanChecksum(actual.getSubstraitPlanChecksum()).build();
        } catch (Exception failure) {
            throw new IllegalStateException("Cannot build actual Substrait exchange producer", failure);
        }
    }
}
