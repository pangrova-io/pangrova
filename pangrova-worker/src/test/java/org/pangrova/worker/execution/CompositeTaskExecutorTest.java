package org.pangrova.worker.execution;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompositeTaskExecutorTest {

    @Test
    void routesCdcTasksToTheWorkerBackendAndNativeTasksToNative() {
        RecordingNative nativeExecutor = new RecordingNative();
        RecordingBackend backend = new RecordingBackend();
        CompositeTaskExecutor executor = new CompositeTaskExecutor(nativeExecutor, backend);

        long workerHandle = executor.createTask(cdcRequest().toByteArray());
        assertThat(executor.isWorkerExecuted(workerHandle)).isTrue();
        assertThat(nativeExecutor.created).isZero();
        assertThat(backend.created).isEqualTo(1);

        executor.startTask(workerHandle);
        assertThat(backend.started).isEqualTo(1);
        assertThat(executor.isFinished(workerHandle)).isTrue();
        assertThat(executor.getTaskStatus(workerHandle)).containsExactly((byte) 7);
        assertThatThrownBy(() -> executor.executeCommand(workerHandle, new byte[]{1}))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(executor.releaseTask(workerHandle)).contains(new byte[]{7});

        long nativeHandle = executor.createTask(nativeRequest().toByteArray());
        assertThat(executor.isWorkerExecuted(nativeHandle)).isFalse();
        assertThat(nativeExecutor.created).isEqualTo(1);
        executor.close();
        assertThat(backend.closed).isTrue();
    }

    private static TaskRequest cdcRequest() {
        return TaskRequest.newBuilder()
                .setAttempt(attempt())
                .setTaskKind(TaskKind.TASK_KIND_CDC_INGEST)
                .build();
    }

    private static TaskRequest nativeRequest() {
        return TaskRequest.newBuilder()
                .setAttempt(attempt())
                .setTaskKind(TaskKind.TASK_KIND_READ)
                .build();
    }

    private static TaskAttemptId attempt() {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query").setTaskId("task")
                .setPlanEpoch(1).setFencingToken(1).setControlEpoch(1).build();
    }

    private static final class RecordingBackend implements WorkerExecutedTaskBackend {
        private int created;
        private int started;
        private boolean closed;

        @Override
        public boolean supports(TaskKind taskKind) {
            return taskKind == TaskKind.TASK_KIND_CDC_INGEST;
        }

        @Override
        public long create(TaskRequest request) {
            created++;
            return 41;
        }

        @Override
        public void start(long handle) {
            started++;
        }

        @Override
        public boolean isFinished(long handle) {
            return true;
        }

        @Override
        public byte[] getTaskStatus(long handle) {
            return new byte[]{7};
        }

        @Override
        public void cancel(long handle) {
        }

        @Override
        public java.util.List<byte[]> getCommitFragments(long handle) {
            return java.util.List.of(new byte[]{8});
        }

        @Override
        public Optional<byte[]> release(long handle) {
            return Optional.of(new byte[]{7});
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class RecordingNative implements TaskExecutor {
        private int created;

        @Override
        public long createTask(byte[] taskRequestBytes) {
            created++;
            return 11;
        }

        @Override
        public void startTask(long executorHandle) {
        }

        @Override
        public Optional<byte[]> releaseTask(long executorHandle) {
            return Optional.empty();
        }

        @Override
        public void cancelTask(long executorHandle) {
        }

        @Override
        public byte[] executeCommand(long executorHandle, byte[] commandProto) {
            return new byte[0];
        }

        @Override
        public boolean isFinished(long executorHandle) {
            return true;
        }

        @Override
        public byte[] getTaskStatus(long executorHandle) {
            return new byte[0];
        }

        @Override
        public byte[] getTaskStats(long executorHandle) {
            return new byte[0];
        }
    }
}
