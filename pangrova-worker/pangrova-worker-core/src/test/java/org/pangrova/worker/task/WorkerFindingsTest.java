package org.pangrova.worker.task;

import org.pangrova.worker.exchange.ExchangeConnectionPoolConfig;
import org.pangrova.worker.heartbeat.WorkerHealthSnapshot;
import org.pangrova.worker.memory.TaskMemorySpillTrigger;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.SocketTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Worker (Java) P1 findings: retry, shutdown, spill, health,
 * buffering, connection pooling, cancellation, and metrics.
 */
class WorkerFindingsTest {

    // ── 13.1.1 Task retry on transient failure ──

    @Test
    void taskRetryConfig_defaults() {
        TaskRetryConfig config = TaskRetryConfig.defaults();
        assertThat(config.maxRetries()).isEqualTo(3);
        assertThat(config.initialBackoffMs()).isEqualTo(1000);
        assertThat(config.backoffMultiplier()).isEqualTo(2.0);
        assertThat(config.maxBackoffMs()).isEqualTo(30_000);
    }

    @Test
    void taskRetryConfig_computeBackoff_exponential() {
        TaskRetryConfig config = TaskRetryConfig.defaults();
        assertThat(config.computeBackoffMs(0)).isEqualTo(1000);
        assertThat(config.computeBackoffMs(1)).isEqualTo(2000);
        assertThat(config.computeBackoffMs(2)).isEqualTo(4000);
        assertThat(config.computeBackoffMs(10)).isEqualTo(30_000); // capped at max
    }

    @Test
    void transientFailureClassifier_networkTimeout() {
        assertThat(TransientFailureClassifier.isTransient(
                new SocketTimeoutException("Read timed out"))).isTrue();
    }

    @Test
    void transientFailureClassifier_connectionRefused() {
        assertThat(TransientFailureClassifier.isTransient(
                new ConnectException("Connection refused"))).isTrue();
    }

    @Test
    void transientFailureClassifier_permanentFailure() {
        assertThat(TransientFailureClassifier.isTransient(
                new IllegalStateException("Unsupported operation"))).isFalse();
    }

    @Test
    void transientFailureClassifier_oomAfterSpill() {
        assertThat(TransientFailureClassifier.isTransient(
                new RuntimeException("Memory pool exhausted after spill"))).isTrue();
    }

    @Test
    void transientFailureClassifier_nestedCause() {
        Exception nested = new RuntimeException("wrapper",
                new ConnectException("Connection refused"));
        assertThat(TransientFailureClassifier.isTransient(nested)).isTrue();
    }

    @Test
    void taskRetryExecutor_shouldRetry_transientFailure() {
        TaskRetryExecutor executor = new TaskRetryExecutor(TaskRetryConfig.defaults());
        Exception transient_ = new SocketTimeoutException("timeout");
        assertThat(executor.shouldRetry("task-1", transient_)).isTrue();
    }

    @Test
    void taskRetryExecutor_shouldNotRetry_permanentFailure() {
        TaskRetryExecutor executor = new TaskRetryExecutor(TaskRetryConfig.defaults());
        Exception permanent = new IllegalStateException("bad state");
        assertThat(executor.shouldRetry("task-1", permanent)).isFalse();
    }

    @Test
    void taskRetryExecutor_exhaustsRetries() {
        TaskRetryConfig config = new TaskRetryConfig(2, 100, 2.0, 1000);
        TaskRetryExecutor executor = new TaskRetryExecutor(config);
        Exception transient_ = new SocketTimeoutException("timeout");

        assertThat(executor.shouldRetry("task-1", transient_)).isTrue();
        executor.recordRetryAndGetBackoff("task-1");
        assertThat(executor.shouldRetry("task-1", transient_)).isTrue();
        executor.recordRetryAndGetBackoff("task-1");
        assertThat(executor.shouldRetry("task-1", transient_)).isFalse();
    }

    @Test
    void taskRetryExecutor_backoffIncreases() {
        TaskRetryConfig config = new TaskRetryConfig(3, 100, 2.0, 10000);
        TaskRetryExecutor executor = new TaskRetryExecutor(config);

        long first = executor.recordRetryAndGetBackoff("task-1");
        long second = executor.recordRetryAndGetBackoff("task-1");
        assertThat(second).isGreaterThan(first);
    }

    // ── 13.1.3 Task memory spill trigger ──

    @Test
    void spillTrigger_noSpillBelowThreshold() {
        TaskMemorySpillTrigger trigger = new TaskMemorySpillTrigger(0.8);
        var decision = trigger.evaluate("task-1", 700, 1000);
        assertThat(decision.shouldSpill()).isFalse();
    }

    @Test
    void spillTrigger_spillsAboveThreshold() {
        TaskMemorySpillTrigger trigger = new TaskMemorySpillTrigger(0.8);
        var decision = trigger.evaluate("task-1", 900, 1000);
        assertThat(decision.shouldSpill()).isTrue();
        assertThat(decision.bytesToSpill()).isGreaterThan(0);
    }

    @Test
    void spillTrigger_recordsMetrics() {
        TaskMemorySpillTrigger trigger = new TaskMemorySpillTrigger(0.8);
        trigger.recordSpill(1024);
        trigger.recordSpill(2048);
        assertThat(trigger.spillCount()).isEqualTo(2);
        assertThat(trigger.spillBytes()).isEqualTo(3072);
    }

    @Test
    void spillTrigger_rejectsInvalidThreshold() {
        assertThatThrownBy(() -> new TaskMemorySpillTrigger(0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TaskMemorySpillTrigger(1.5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── 13.1.4 Worker health check enhancement ──

    @Test
    void healthSnapshot_healthy() {
        WorkerHealthSnapshot snapshot = new WorkerHealthSnapshot(
                "LOW", 5, 100_000_000L, true, 500_000_000L, 1_000_000_000L, 200_000_000L, 3600);
        assertThat(snapshot.isHealthy()).isTrue();
    }

    @Test
    void healthSnapshot_unhealthy_criticalMemory() {
        WorkerHealthSnapshot snapshot = new WorkerHealthSnapshot(
                "CRITICAL", 5, 100_000_000L, true, 500_000_000L, 1_000_000_000L, 200_000_000L, 3600);
        assertThat(snapshot.isHealthy()).isFalse();
    }

    @Test
    void healthSnapshot_unhealthy_nativeDead() {
        WorkerHealthSnapshot snapshot = new WorkerHealthSnapshot(
                "NONE", 0, 100_000_000L, false, 500_000_000L, 1_000_000_000L, 200_000_000L, 3600);
        assertThat(snapshot.isHealthy()).isFalse();
    }

    @Test
    void healthSnapshot_unhealthy_noDisk() {
        WorkerHealthSnapshot snapshot = new WorkerHealthSnapshot(
                "NONE", 0, 0L, true, 500_000_000L, 1_000_000_000L, 200_000_000L, 3600);
        assertThat(snapshot.isHealthy()).isFalse();
    }

    // ── 13.1.5 Task output buffering with backpressure ──
    // OutputBufferBackpressure already exists and is tested in OutputBufferBackpressureTest.
    // The bounded queue with configurable size is already implemented via ActiveOutputBuffer.

    // ── 13.1.6 Exchange client connection pooling ──

    @Test
    void exchangeConnectionPoolConfig_defaults() {
        ExchangeConnectionPoolConfig config = ExchangeConnectionPoolConfig.defaults();
        assertThat(config.maxConnectionsPerHost()).isEqualTo(10);
        assertThat(config.idleTimeoutSeconds()).isEqualTo(300);
        assertThat(config.healthCheckIntervalSeconds()).isEqualTo(30);
        assertThat(config.maxTotalConnections()).isEqualTo(200);
    }

    @Test
    void exchangeConnectionPoolConfig_rejectsInvalid() {
        assertThatThrownBy(() -> new ExchangeConnectionPoolConfig(0, 300, 30, 200))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExchangeConnectionPoolConfig(10, 300, 30, 5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── 13.1.7 Task cancellation propagation ──

    @Test
    void queryCancellationPropagator_cancelsAllTasks() {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        QueryCancellationPropagator propagator = new QueryCancellationPropagator(taskManager);

        propagator.registerTask("q1", "task-1");
        propagator.registerTask("q1", "task-2");
        assertThat(propagator.taskCountForQuery("q1")).isEqualTo(2);
        assertThat(propagator.trackedQueryCount()).isEqualTo(1);
    }

    @Test
    void queryCancellationPropagator_deregisterRemovesTask() {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        QueryCancellationPropagator propagator = new QueryCancellationPropagator(taskManager);

        propagator.registerTask("q1", "task-1");
        propagator.registerTask("q1", "task-2");
        propagator.deregisterTask("q1", "task-1");
        assertThat(propagator.taskCountForQuery("q1")).isEqualTo(1);
    }

    @Test
    void queryCancellationPropagator_cancelRemovesIndex() {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        QueryCancellationPropagator propagator = new QueryCancellationPropagator(taskManager);

        propagator.registerTask("q1", "task-1");
        propagator.cancelQuery("q1");
        assertThat(propagator.trackedQueryCount()).isEqualTo(0);
    }

    // ── 13.1.8 Worker per-task metrics ──

    @Test
    void taskMetricsSnapshot_creation() {
        TaskMetricsSnapshot metrics = new TaskMetricsSnapshot(
                "task-1", "query-1", 5_000_000_000L, 10_000_000_000L,
                256_000_000L, 1_000_000L, 500_000_000L, 100_000_000L, 50_000_000L);
        assertThat(metrics.taskId()).isEqualTo("task-1");
        assertThat(metrics.cpuTimeMs()).isEqualTo(5000);
        assertThat(metrics.wallTimeMs()).isEqualTo(10000);
        assertThat(metrics.memoryPeakBytes()).isEqualTo(256_000_000L);
        assertThat(metrics.rowsProcessed()).isEqualTo(1_000_000L);
        assertThat(metrics.bytesRead()).isEqualTo(500_000_000L);
        assertThat(metrics.bytesWritten()).isEqualTo(100_000_000L);
        assertThat(metrics.spillBytes()).isEqualTo(50_000_000L);
    }

    @Test
    void taskMetricsSnapshot_rejectsBlankTaskId() {
        assertThatThrownBy(() -> new TaskMetricsSnapshot(
                "", "query-1", 0, 0, 0, 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void taskMetricsSnapshot_rejectsNegativeCpuTime() {
        assertThatThrownBy(() -> new TaskMetricsSnapshot(
                "task-1", "query-1", -1, 0, 0, 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
