package org.pangrova.testing.e2e.shuffle;

import java.nio.file.Path;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.exchange.remote.RemoteShuffleByteEgress;
import org.pangrova.worker.exchange.remote.RemoteShuffleChannelAdmission;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.task.TaskManagerContext;
import org.pangrova.worker.task.memory.MemoryRefs;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual Worker admission/reporting with real JNI runtime and durable mTLS service, not successful SQL E2E. */
@Tag("native-required")
@Tag("native-runtime-owner")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
@ResourceLock(ActualShuffleBackend.CREDENTIAL_LOCK)
class WorkerShuffleChannelAdmissionIntegrationTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test void allProducerEndpointsAreAdmittedBeforeNativeCreationAndEmitCanonicalAdmissionFailure() throws Exception {
        try (var service = new ActualShuffleService(directory, 2, 2);
             var runtime = new ActualNativeTaskFixture()) {
            var request = service.registerProducer(runtime::bindWorker).request();
            var admission = new RemoteShuffleChannelAdmission(new RemoteShuffleChannelAdmission.Limits(1, 1));
            var egress = new RemoteShuffleByteEgress(service.clientPool(admission));
            var executor = new NativeTaskExecutor(egress);
            var observed = new CopyOnWriteArrayList<TaskStatusReport>();
            var resources = new ResourceManager(runtime.arbitrator(), new ThreadBudgetTracker(8, 4, 2),
                    new SpillManager());
            var monitor = Executors.newSingleThreadExecutor(task -> new Thread(task, "actual-channel-admission"));
            long available = runtime.arbitrator().getAvailableMemory();
            try (var manager = new TaskManager(new TaskManagerContext(observed::add, executor,
                    new MemoryRefs(resources, runtime.nodePool(), null), runtime.transports(), monitor),
                    "actual-channel-admission-worker", 0.0, runtime.admissionIncarnation())) {
                var envelope = SubmitTaskRequest.from(request);
                var id = manager.submitTask(TaskRequestDtoConverter.toTaskDescriptor(envelope), envelope);
                assertThat(manager.getTaskSpiState(id)).contains(org.pangrova.spi.execution.TaskStatus.TaskState.FAILED);
                assertThat(manager.getExecutorHandle(id)).isEmpty();
                var failed = observed.stream().filter(report -> "FAILED".equals(report.state())).findFirst().orElseThrow();
                var canonical = TaskReport.parseFrom(failed.canonicalTaskReport());
                assertThat(canonical.getAttempt()).isEqualTo(request.getAttempt());
                assertThat(canonical.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
                assertThat(canonical.getError().getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_ADMISSION);
                assertThat(canonical.getError().getCode()).isEqualTo("SHUFFLE_MANAGED_CHANNEL_LIMIT");
                assertThat(canonical.getError().getRetryable()).isTrue();
                assertThat(canonical.getError().getFailedComponent()).isEqualTo("remote-shuffle-channel-admission");
                assertThat(canonical.getCoverage().getCompletedWorkUnits()).isZero();
                assertThat(runtime.nodePool().getUsedBytes()).isZero();
                assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(available);
                for (int endpoint = 0; endpoint < 2; endpoint++) assertThat(service.wire(endpoint).opened).isEmpty();
                assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
            }
            admission.requireDrained();
        }
    }
}
