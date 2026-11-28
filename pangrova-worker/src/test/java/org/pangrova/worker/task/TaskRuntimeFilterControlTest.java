package org.pangrova.worker.task;

import java.io.ByteArrayInputStream;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.task.TaskRuntimeFilterWire;
import org.pangrova.protocol.v2.RuntimeFilterBinding;
import org.pangrova.protocol.v2.RangeFilter;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.RuntimeFilterScope;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.enums.TaskState;
import org.pangrova.worker.http.endpoint.TaskResource;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises Worker delivery against an actual JNI handle at the runner startup boundary. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskRuntimeFilterControlTest {
    @Test
    void acknowledgesOnlyTheExactRunningOwnerAndRejectsASealedAttempt() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "runtime-filter");
                var resource = new TaskResource(tasks.manager(), runtime.transports().exchangeTransport(),
                        "127.0.0.1", runtime.resultPort())) {
            var request = runtime.valuesRequest(false);
            var info = tasks.preparePending(request);
            var attempt = request.getAttempt();

            assertThat(deliver(resource, attempt)).isEqualTo(409);
            assertThat(tasks.manager().transition(info.taskId(), TaskState.RUNNING)).isTrue();
            assertThat(deliver(resource, attempt.toBuilder().setAttemptId(attempt.getAttemptId() + 1).build()))
                    .isEqualTo(409);
            assertThat(deliver(resource, attempt)).isEqualTo(204);

            assertThat(tasks.manager().cancelTask(info.taskId()).isSuccess()).isTrue();
            tasks.manager().releaseTaskExecutor(info.taskId());
            assertThat(info.executorHandle()).isZero();
            assertThat(deliver(resource, attempt)).isEqualTo(409);
        }
    }

    private static int deliver(TaskResource resource, TaskAttemptId attempt) throws Exception {
        var update = RuntimeFilterBinding.newBuilder().setSoleTableScan(true)
                .addFilters(RuntimeFilter.newBuilder().setColumnName("id")
                        .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_ROW_FILTER)
                        .setRange(RangeFilter.newBuilder()
                                .setLower(TypedValue.newBuilder().setInt64Value(1))
                                .setUpper(TypedValue.newBuilder().setInt64Value(5))
                                .setLowerInclusive(true).setUpperInclusive(true))).build();
        try (Response response = resource.injectDynamicFilter(attempt.getTaskId(),
                new ByteArrayInputStream(TaskRuntimeFilterWire.encode(attempt, update)), null)) {
            return response.getStatus();
        }
    }
}
