package org.pangrova.testing.integration.e2e.shuffle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ReadPartitionRequest;
import org.pangrova.shuffle.exchange.ExchangeInput;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.nativefixture.ActualNativeResultReader;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.exchange.CanonicalBatchChunks;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.exchange.remote.RemoteShuffleByteEgress;
import org.pangrova.worker.exchange.remote.RemoteShuffleExchangeSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual durable service → JNI ExchangeNode → Flight. Network faults never synthesize business ACKs. */
@Tag("native-required")
@Tag("native-runtime-owner")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
@ResourceLock(ActualShuffleBackend.CREDENTIAL_LOCK)
class NativeExchangeTransportExternalIntegrationTest extends ContainerTestBase {
    @TempDir Path directory;
    private ActualShuffleService service;
    private ActualNativeTaskFixture nativeRuntime;
    private NativeExchangeTransport transport;

    @BeforeEach void start() throws Exception {
        service = new ActualShuffleService(directory, 1, 1);
        nativeRuntime = new ActualNativeTaskFixture();
        transport = new NativeExchangeTransport(nativeRuntime.executor(), service.clientPool());
    }

    @AfterEach void close() throws Exception {
        if (service != null) service.wire(0).resume.countDown();
        Exception failure = null;
        for (AutoCloseable owner : new AutoCloseable[]{transport, nativeRuntime, service}) {
            if (owner == null) continue;
            try { owner.close(); } catch (Exception cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
        }
        if (failure != null) throw failure;
    }

    @Test void forwardsVerifiedChunksAndTerminalUsingTaskAttempt() throws Exception {
        verifyOriginalOpaqueChunkBoundary();
        var producer = service.registerProducer();
        int rows = 137_000;
        byte[] actualIpc = ActualExternalShuffleTask.arrowBatch(rows);
        assertThat(actualIpc.length).isGreaterThan(CanonicalBatchChunks.MAX_CHUNK_BYTES);
        publish(producer, actualIpc, rows);
        var request = nativeRuntime.bindWorker(ActualExternalShuffleTask.consumer(producer));
        var binding = service.delegateRead(producer, request);
        long handle = nativeRuntime.createPending(request);
        transport.bindInputs(handle, binding.consumerAttempt(), List.of(input(binding)));
        nativeRuntime.executor().startTask(handle);
        var values = ActualNativeResultReader.count(nativeRuntime, request);
        assertThat(values.rows()).isEqualTo(rows);
        assertThat(values.sum()).isEqualTo((long) rows * (rows - 1) / 2);
        var report = nativeRuntime.awaitFinished(handle);
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(report.hasError()).isFalse();
        assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
        assertReadAuthority(request.getAttempt(), binding.readAccess().token());
        transport.releaseInputs(handle);
        var released = TaskReport.parseFrom(nativeRuntime.executor().releaseTask(handle).orElseThrow());
        assertThat(released.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(released.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(released.hasNativeTaskStats()).isTrue();
    }

    private void verifyOriginalOpaqueChunkBoundary() throws Exception {
        byte[] batch = new byte[CanonicalBatchChunks.MAX_CHUNK_BYTES + 7];
        Arrays.fill(batch, (byte) 7);
        var producer = service.registerProducer();
        publish(producer, batch, 1);
        var consumer = nativeRuntime.bindWorker(ActualExternalShuffleTask.consumer(producer));
        var binding = service.delegateRead(producer, consumer);
        var chunks = new ArrayList<org.pangrova.protocol.v2.BatchChunk>();
        try (var pool = service.clientPool();
             var source = new RemoteShuffleExchangeSource(binding, pool.borrow(binding.selectedEndpoint()))) {
            org.pangrova.protocol.v2.BatchChunk chunk;
            while ((chunk = source.nextVerifiedChunk()) != null) chunks.add(chunk);
            assertThat(source.isFinished()).isTrue();
            assertThat(source.lastCompletedSequence()).isZero();
        }
        assertThat(chunks).extracting(chunk -> chunk.getChunkIndex()).containsExactly(0, 1);
        assertThat(chunks).extracting(chunk -> chunk.getData().size())
                .containsExactly(CanonicalBatchChunks.MAX_CHUNK_BYTES, 7);
        var actualBytes = new java.io.ByteArrayOutputStream();
        for (var chunk : chunks) chunk.getData().writeTo(actualBytes);
        assertThat(actualBytes.toByteArray()).isEqualTo(batch);
        assertReadAuthority(consumer.getAttempt(), binding.readAccess().token());
        // Opaque service payload verifies only the original byte/chunk contract,
        // never Arrow validity or successful Native SQL execution.
    }

    @Test void reportsCorruptionWhenRpcCompletesBeforeTerminal() throws Exception {
        var producer = service.registerProducer();
        publish(producer, ActualExternalShuffleTask.arrowBatch(3), 3);
        service.wire(0).truncateBeforeReadTerminal = true;
        var request = nativeRuntime.bindWorker(ActualExternalShuffleTask.consumer(producer));
        var binding = service.delegateRead(producer, request);
        long handle = nativeRuntime.createPending(request);
        transport.bindInputs(handle, binding.consumerAttempt(), List.of(input(binding)));
        nativeRuntime.executor().startTask(handle);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(TaskReport.parseFrom(nativeRuntime.executor().getTaskStatus(handle)).getState())
                        .isEqualTo(TaskState.TASK_STATE_FAILED));
        var report = TaskReport.parseFrom(nativeRuntime.executor().getTaskStatus(handle));
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
        assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(report.getError().getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_DATA_CORRUPTION);
        assertThat(report.getError().getCode()).isEqualTo("EXTERNAL_SHUFFLE_CORRUPTION");
        assertThat(report.getError().getRetryable()).isFalse();
        assertThat(report.getError().getFailedComponent()).isEqualTo("external-shuffle-ingress");
        assertThat(report.getError().getMessage()).contains("ended before its terminal frame");
        assertThat(service.wire(0).responses).anySatisfy(frame ->
                assertThat(frame.message() instanceof ReadBatchResponse response && response.hasTerminal()).isTrue());
        transport.releaseInputs(handle);
        var released = TaskReport.parseFrom(nativeRuntime.executor().releaseTask(handle).orElseThrow());
        assertThat(released.getError()).isEqualTo(report.getError());
    }

    @Test void releaseInputsCancelsTheActivePartitionRpc() throws Exception {
        var producer = service.registerProducer();
        publish(producer, ActualExternalShuffleTask.arrowBatch(3), 3);
        service.wire(0).pauseReadOutput = true;
        var request = nativeRuntime.bindWorker(ActualExternalShuffleTask.consumer(producer));
        var binding = service.delegateRead(producer, request);
        long handle = nativeRuntime.createPending(request);
        transport.bindInputs(handle, binding.consumerAttempt(), List.of(input(binding)));
        assertThat(service.wire(0).paused.await(5, TimeUnit.SECONDS)).isTrue();
        nativeRuntime.executor().cancelTask(handle);
        transport.releaseInputs(handle);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(service.wire(0).cancelled).contains("ReadPartition"));
        var report = TaskReport.parseFrom(nativeRuntime.executor().getTaskStatus(handle));
        assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
        assertThat(report.hasError()).isFalse(); // Revocation did not inject a false successful EOF or corruption.
        var released = TaskReport.parseFrom(nativeRuntime.executor().releaseTask(handle).orElseThrow());
        assertThat(released.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(released.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
        assertThat(nativeRuntime.executor().releaseTask(handle)).isEmpty();
        assertThat(nativeRuntime.nodePool().getUsedBytes()).isZero();
        assertThat(nativeRuntime.arbitrator().getUsedMemory()).isZero();
    }

    private void publish(ActualShuffleService.AdmittedProducer producer, byte[] payload, long rows) {
        var request = producer.request();
        try (var egress = new RemoteShuffleByteEgress(service.clientPool())) {
            assertThat(egress.installTask(request)).isTrue();
            egress.onPartitionBatch(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId(),
                    0, rows, ByteBuffer.wrap(payload));
            egress.onTaskCompleted(request.getAttempt().getQueryOrJobId(), request.getAttempt().getTaskId());
        }
        service.finalizeProducer(producer);
    }

    private void assertReadAuthority(org.pangrova.protocol.v2.TaskAttemptId attempt, byte[] token) {
        assertThat(service.wire(0).requests).anySatisfy(frame -> {
            assertThat(frame.message()).isInstanceOf(ReadPartitionRequest.class);
            var read = (ReadPartitionRequest) frame.message();
            assertThat(read.getConsumerAttempt()).isEqualTo(attempt);
            assertThat(read.getAccessCapability().getToken().toByteArray()).isEqualTo(token);
        });
    }

    private static ExchangeInput input(org.pangrova.shuffle.exchange.ExternalShuffleTransport binding) {
        return new ExchangeInput(ActualExternalShuffleTask.PLAN_NODE, ActualExternalShuffleTask.EXCHANGE_ID, 1, binding);
    }
}
