/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.testing.integration.e2e.index;

import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.types.value.TypedRow;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Observes actual issued plans and Native drain reports before the Coordinator compacts them. */
public abstract class NativeIndexE2ETestBase extends SqlE2ETestBase {
    private static EvidenceObserver observer;

    /** The observer copies the original issued request before drain receipt compaction. */
    protected static QueryEvidence executeObserved(String sql) {
        EvidenceObserver current;
        synchronized (NativeIndexE2ETestBase.class) {
            if (observer == null || observer.tracker != taskTracker) {
                observer = new EvidenceObserver(taskTracker);
                taskTracker.addExecutionDrainListener(observer::accept);
            }
            current = observer;
        }
        Capture capture = new Capture();
        current.active.add(capture);
        try {
            var submitted = submitQueryViaHttp(sql);
            var endpoint = awaitResultEndpoint(submitted.queryId(), submitted);
            String resultTaskId = requireNativeResultTaskId(endpoint, submitted.queryId(), taskTracker);
            var issuedBeforeConsumption = new HashSet<>(taskTracker.getTaskIds(submitted.queryId()));
            issuedBeforeConsumption.add(resultTaskId);
            List<TypedRow> rows = consumeSubmittedQuery(submitted);
            org.awaitility.Awaitility.await("actual Native query drain")
                    .atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
                    .untilAsserted(() -> {
                        var tasks = capture.forQuery(submitted.queryId());
                        assertThat(tasks).isNotEmpty();
                        assertThat(tasks.stream().map(task -> task.request().getAttempt().getTaskId()))
                                .containsAll(issuedBeforeConsumption);
                    });
            var tasks = capture.forQuery(submitted.queryId());
            for (IssuedTask task : tasks) {
                assertThat(task.report().getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
                assertThat(task.report().getReportOrigin())
                        .isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
                assertThat(task.report().getExecutionDrainState())
                        .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
                assertThat(task.report().hasNativeTaskStats()).isTrue();
                assertThat(task.report().getAttempt()).isEqualTo(task.request().getAttempt());
            }
            return new QueryEvidence(rows, tasks);
        } catch (Exception failure) {
            throw new IllegalStateException("Real SQL/Flight execution did not complete", failure);
        } finally {
            current.active.remove(capture);
        }
    }

    /** Sums pre-scan index decisions from the actual tasks for this query. */
    protected static NativeScanResult executeNativeScan(String sql) {
        var execution = executeObserved(sql);
        long evaluated = 0;
        long skipped = 0;
        for (var task : execution.tasks()) {
            var stats = task.report().getNativeTaskStats();
            evaluated = Math.addExact(evaluated, stats.getIndexSplitsEvaluated());
            skipped = Math.addExact(skipped, stats.getIndexSplitsSkipped());
        }
        return new NativeScanResult(execution.rows(), evaluated, skipped);
    }

    protected record NativeScanResult(List<TypedRow> rows, long evaluated, long skipped) {}

    public record IssuedTask(TaskRequest request, TaskReport report) {}
    public record QueryEvidence(List<TypedRow> rows, List<IssuedTask> tasks) {}

    private static final class EvidenceObserver {
        private final CoordinatorTaskTracker tracker;
        private final CopyOnWriteArrayList<Capture> active = new CopyOnWriteArrayList<>();

        private EvidenceObserver(CoordinatorTaskTracker tracker) { this.tracker = tracker; }

        private void accept(TaskReport report) {
            if (active.isEmpty()) return;
            var issued = tracker.getTaskRequest(report.getAttempt().getTaskId());
            if (issued != null) {
                var fact = new IssuedTask(issued.taskRequest(), report);
                active.forEach(capture -> capture.accept(fact));
            }
        }
    }

    /** One bounded query scope; replay replaces the same task, and close drops retained plans. */
    private static final class Capture {
        private final Map<String, IssuedTask> tasks = new LinkedHashMap<>();
        private long bytes;
        private IllegalStateException failure;

        private synchronized void accept(IssuedTask fact) {
            if (failure != null) return;
            String id = fact.request().getAttempt().getTaskId();
            if (tasks.containsKey(id)) return;
            bytes += fact.request().getSerializedSize() + fact.report().getSerializedSize();
            if (tasks.size() >= 512 || bytes > 32L * 1024 * 1024) {
                // Observation must not reject a genuine execution-drain notification.
                // Fail the test at its own boundary while releasing captured plans.
                failure = new IllegalStateException("E2E evidence capture exceeded its explicit bound");
                tasks.clear();
                bytes = 0;
                return;
            }
            tasks.put(id, fact);
        }

        private synchronized List<IssuedTask> forQuery(String queryId) {
            if (failure != null) throw failure;
            return tasks.values().stream().filter(task -> task.request().getAttempt()
                    .getQueryOrJobId().equals(queryId)).toList();
        }
    }
}
