package org.pangrova.streaming.worker;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;
import org.pangrova.protocol.v2.CdcIngestBinding;
import org.pangrova.protocol.v2.CdcSourceRange;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.enums.CdcOperation;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.cdc.CdcRangeBatchWriter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CdcWorkerTaskBackendTest {

    private static final String STREAM = "stream-1";
    private static final String TENANT = "tenant-a";
    private static final CdcRecordConverter CONVERTER = new CdcRecordConverter() {
        @Override
        public String formatId() {
            return "fixture";
        }

        @Override
        public CdcEnvelope deserialize(byte[] payload) {
            return new CdcEnvelope(CdcOperation.INSERT, "target", null,
                    CdcRow.of(Map.of("payload", new String(payload, StandardCharsets.UTF_8))),
                    Instant.EPOCH, 0, Map.of());
        }
    };

    @Test
    void executesExactRangesAndReturnsTheCanonicalWorkerReceipt() throws Exception {
        List<List<CdcEnvelope>> written = new ArrayList<>();
        CdcWorkerTaskBackend backend = new CdcWorkerTaskBackend(
                request -> new Resources(reader(), envelopes -> {
                    written.add(List.copyOf(envelopes));
                    return List.of(new byte[]{1});
                }), options());
        long handle = backend.create(request());

        backend.start(handle);
        awaitFinished(backend, handle);

        TaskReport report = TaskReport.parseFrom(backend.getTaskStatus(handle));
        assertThat(report.getAttempt()).isEqualTo(request().getAttempt());
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_EXECUTED);
        assertThat(report.getExecutionDrainState()).isEqualTo(
                ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED);
        assertThat(report.hasNativeTaskStats()).isFalse();
        assertThat(backend.getCommitFragments(handle)).containsExactly(new byte[]{1});
        assertThat(written).hasSize(1);
        assertThat(written.get(0)).hasSize(2);
        assertThat(backend.release(handle)).contains(report.toByteArray());
        assertThat(backend.release(handle)).isEmpty();
        backend.close();
    }

    @Test
    void rejectsSourceOperationOutsideAdmittedWriteScopeBeforeStaging() throws Exception {
        try (var backend = new CdcWorkerTaskBackend(request -> new Resources(reader(), envelopes -> {
            throw new AssertionError("Unauthorized source operation reached the connector sink");
        }), options())) {
            TaskRequest request = request().toBuilder().setCdcIngest(request().getCdcIngest().toBuilder()
                    .clearAllowedOperations()
                    .addAllowedOperations(org.pangrova.protocol.v2.CdcWriteOperation.CDC_WRITE_OPERATION_UPDATE))
                    .build();
            long handle = backend.create(request);
            backend.start(handle);
            awaitFinished(backend, handle);

            TaskReport report = TaskReport.parseFrom(backend.getTaskStatus(handle));
            assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
            assertThat(report.getError().getMessage()).isEqualTo("CDC stream is not authorized for INSERT");
            assertThat(backend.getCommitFragments(handle)).isEmpty();
        }
    }

    @Test
    void cancellationProducesAnUnsuccessfulNoNativeOwnerReceipt() {
        CdcWorkerTaskBackend backend = new CdcWorkerTaskBackend(
                request -> new Resources(reader(), envelopes -> List.of()), options());
        long handle = backend.create(request());

        backend.cancel(handle);

        assertThat(backend.isFinished(handle)).isTrue();
        try {
            TaskReport report = TaskReport.parseFrom(backend.getTaskStatus(handle));
            assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
            assertThat(report.getReportOrigin()).isEqualTo(
                    TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
            assertThat(report.getExecutionDrainState()).isEqualTo(
                    ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
            assertThat(report.hasError()).isTrue();
            assertThat(report.hasNativeTaskStats()).isFalse();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new AssertionError(failure);
        }
        backend.close();
    }

    @Test
    void cancellationDoesNotAdvertiseDrainWhileTaskResourcesAreClosing() throws Exception {
        var closing = new java.util.concurrent.CountDownLatch(1);
        var allowClose = new java.util.concurrent.CountDownLatch(1);
        try (var backend = new CdcWorkerTaskBackend(
                request -> new Resources(reader(), envelopes -> List.of(), () -> {
                    closing.countDown();
                    boolean interrupted = false;
                    for (;;) {
                        try {
                            allowClose.await();
                            break;
                        } catch (InterruptedException ignored) {
                            interrupted = true;
                        }
                    }
                    if (interrupted) Thread.currentThread().interrupt();
                }), options())) {
            long handle = backend.create(request());
            backend.start(handle);
            try {
                assertThat(closing.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                backend.cancel(handle);
                assertThat(backend.isFinished(handle)).isFalse();
            } finally {
                allowClose.countDown();
            }
            awaitFinished(backend, handle);
            assertThat(TaskReport.parseFrom(backend.release(handle).orElseThrow()).getState())
                    .isEqualTo(TaskState.TASK_STATE_CANCELLED);
        }
    }

    @Test
    void resourceOpenFailureProducesAFailedReceipt() throws Exception {
        CdcWorkerTaskBackend backend = new CdcWorkerTaskBackend(
                request -> {
                    throw new IllegalStateException("source credentials rejected");
                }, options());
        long handle = backend.create(request());

        backend.start(handle);
        awaitFinished(backend, handle);

        TaskReport report = TaskReport.parseFrom(backend.getTaskStatus(handle));
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
        assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
        assertThat(report.getError().getCode()).isEqualTo("CDC_INGEST_FAILED");
        assertThat(report.getError().getMessage()).contains("credentials rejected");
        backend.close();
    }

    private static void awaitFinished(CdcWorkerTaskBackend backend, long handle) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (!backend.isFinished(handle) && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(backend.isFinished(handle)).isTrue();
    }

    private static CdcWorkerTaskOptions options() {
        return new CdcWorkerTaskOptions(10, Duration.ofMillis(1), Duration.ofSeconds(1));
    }

    private static CdcSourceReader reader() {
        return new CdcSourceReader() {
            private boolean delivered;

            @Override
            public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
                if (delivered) {
                    return List.of();
                }
                delivered = true;
                return List.of(
                        new SourceRecord("orders", 0, 10, null, "10".getBytes(StandardCharsets.UTF_8), 0),
                        new SourceRecord("orders", 0, 11, null, "11".getBytes(StandardCharsets.UTF_8), 0));
            }

            @Override
            public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
                throw new AssertionError("Worker CDC must not acknowledge source offsets");
            }

            @Override
            public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
            }

            @Override
            public Map<TopicPartitionOffset, Long> endOffsets(
                    Collection<TopicPartitionOffset> partitions) {
                return Map.of(new TopicPartitionOffset("orders", 0), 12L);
            }

            @Override
            public void close() {
            }
        };
    }

    private static TaskRequest request() {
        return TaskRequest.newBuilder()
                .setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId("query-1").setTaskId("task-1")
                        .setPlanEpoch(1).setFencingToken(7).setControlEpoch(1))
                .setTaskKind(TaskKind.TASK_KIND_CDC_INGEST)
                .setSecurity(SecurityContext.newBuilder()
                        .setTenantId(TENANT).setPrincipal("test-user"))
                .setCdcIngest(CdcIngestBinding.newBuilder()
                        .setStreamId(STREAM).setTenantId(TENANT)
                        .setSourceOwnershipEpoch("epoch-1")
                        .setSourceProvider("fixture").setFormatId("fixture")
                        .addAllowedOperations(org.pangrova.protocol.v2.CdcWriteOperation.CDC_WRITE_OPERATION_INSERT)
                        .addRanges(CdcSourceRange.newBuilder()
                                .setTopic("orders").setPartition(0)
                                .setStartOffset(10).setEndOffset(12)))
                .build();
    }

    private record Resources(
            CdcSourceReader reader, CdcRangeBatchWriter writer, InterruptibleClose onClose)
            implements CdcWorkerTaskResources {
        private Resources(CdcSourceReader reader, CdcRangeBatchWriter writer) {
            this(reader, writer, () -> {});
        }
        @Override
        public CdcSourceReader sourceReader() {
            return reader;
        }

        @Override
        public CdcRecordConverter converter() {
            return CONVERTER;
        }

        @Override
        public CdcRangeBatchWriter writer() {
            return writer;
        }

        @Override
        public void close() {
            try {
                onClose.run();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                reader.close();
            }
        }
    }

    @FunctionalInterface
    private interface InterruptibleClose {
        void run() throws InterruptedException;
    }
}
