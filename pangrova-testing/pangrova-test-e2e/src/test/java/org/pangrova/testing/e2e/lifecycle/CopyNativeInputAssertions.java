package org.pangrova.testing.e2e.lifecycle;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.copy.CopySourceStagingCommand;
import org.pangrova.worker.copy.CopyStageLeases;
import org.pangrova.worker.execution.NativeTaskCreationFailure;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.LongFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Real frozen managed object -> owned local input -> packaged JNI -> Native Flight values. */
final class CopyNativeInputAssertions {
    private CopyNativeInputAssertions() {}

    static void verify(CopySourceStagingCommand command, LongFunction<SubmitTaskRequest> tasks,
                       Path stagingRoot, long fileSize) throws Exception {
        try (var inputs = new CopyStageLeases(command, null);
             var runtime = new ActualNativeTaskFixture()) {
            var executor = runtime.executor();
            executor.setCopyInputs(inputs);
            var denied = tasks.apply(0);
            var failure = catchThrowable(() -> executor.createCopyTask(denied));
            assertThat(failure).isInstanceOf(NativeTaskCreationFailure.class);
            var noOwner = (NativeTaskCreationFailure) failure;
            assertThat(noOwner.withoutNativeOwner()).isTrue();
            assertThat(noOwner.workerObjectReadBytes()).isZero();
            assertThat(localInputs(stagingRoot)).isEmpty();

            var exhausted = tasks.apply(fileSize);
            long exhaustedHandle = executor.createCopyTask(exhausted);
            assertThat(localInputs(stagingRoot)).hasSize(1);
            executor.startTask(exhaustedHandle);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(() ->
                    TaskReport.parseFrom(executor.getTaskStatus(exhaustedHandle)).getState()
                            == TaskState.TASK_STATE_FAILED);
            var exhaustedReport = TaskReport.parseFrom(executor.getTaskStatus(exhaustedHandle));
            assertThat(exhaustedReport.getUsage().getObjectStoreReadBytes()).isEqualTo(fileSize);
            assertThat(localInputs(stagingRoot)).hasSize(1);
            Path privateDirectory = localInputs(stagingRoot).get(0).getParent();
            Path cleanupBarrier = Files.writeString(privateDirectory.resolve("cleanup-barrier"), "retained");
            assertThat(catchThrowable(() -> executor.releaseTask(exhaustedHandle)))
                    .isInstanceOf(NativeTaskCreationFailure.class);
            assertThat(privateDirectory).exists();
            Files.delete(cleanupBarrier);
            var exhaustedRelease = TaskReport.parseFrom(executor.releaseTask(exhaustedHandle).orElseThrow());
            assertThat(exhaustedRelease.getUsage().getObjectStoreReadBytes()).isEqualTo(fileSize);
            assertThat(localInputs(stagingRoot)).isEmpty();

            var cancelled = tasks.apply(Math.addExact(8 * fileSize, 1024 * 1024));
            long cancelledHandle = executor.createCopyTask(cancelled);
            executor.cancelTask(cancelledHandle);
            assertThat(localInputs(stagingRoot)).hasSize(1);
            var cancelledRelease = TaskReport.parseFrom(executor.releaseTask(cancelledHandle).orElseThrow());
            assertThat(cancelledRelease.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
            assertThat(cancelledRelease.getUsage().getObjectStoreReadBytes()).isEqualTo(fileSize);
            assertThat(localInputs(stagingRoot)).isEmpty();

            verifyTaskManager(runtime, tasks, stagingRoot, fileSize);
        }
    }

    private static void verifyTaskManager(ActualNativeTaskFixture runtime,
            LongFunction<SubmitTaskRequest> tasks, Path stagingRoot, long fileSize) throws Exception {
        var reports = new java.util.concurrent.CopyOnWriteArrayList<org.pangrova.protocol.rest.task.TaskStatusReport>();
        var resources = new org.pangrova.resource.ResourceManager(runtime.arbitrator(),
                new org.pangrova.resource.thread.ThreadBudgetTracker(8, 4, 2),
                new org.pangrova.resource.spill.SpillManager());
        var monitor = java.util.concurrent.Executors.newSingleThreadExecutor();
        var context = new org.pangrova.worker.task.TaskManagerContext(reports::add, runtime.executor(),
                new org.pangrova.worker.task.memory.MemoryRefs(resources, runtime.nodePool(), null),
                runtime.transports(), monitor);
        try (var manager = new org.pangrova.worker.task.TaskManager(context, "actual-copy-worker", 0.0)) {
            var mismatched = tasks.apply(Math.addExact(8 * fileSize, 1024 * 1024));
            var changed = mismatched.taskRequest().toBuilder();
            changed.getSplitsBuilder(0).setSourceSnapshot("different-version");
            var rejected = SubmitTaskRequest.from(changed.build(), mismatched.copySourceRead());
            String rejectedId = submit(manager, rejected);
            var reported = awaitReleasedReport(reports, rejected.taskRequest(), TaskState.TASK_STATE_FAILED);
            assertThat(manager.getTaskState(rejectedId)).contains(org.pangrova.worker.task.TaskManager.TaskState.FAILED);
            assertThat(reported.getUsage().getObjectStoreReadBytes()).isEqualTo(fileSize);
            assertThat(reported.getExecutionDrainState()).isEqualTo(org.pangrova.protocol.v2.ExecutionDrainState
                    .EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
            assertThat(localInputs(stagingRoot)).isEmpty();

            var accepted = tasks.apply(Math.addExact(8 * fileSize, 1024 * 1024));
            String id = submit(manager, accepted);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20))
                    .until(() -> manager.getExecutorHandle(id).orElse(0) > 0);
            assertThat(localInputs(stagingRoot)).hasSize(1);
            long handle = manager.getExecutorHandle(id).orElseThrow();
            readExactValues(runtime, accepted.taskRequest());
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20))
                    .until(() -> manager.getTaskState(id).orElseThrow()
                            == org.pangrova.worker.task.TaskManager.TaskState.FINISHED);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20))
                    .until(() -> runtime.drainEvidence(handle, accepted.taskRequest()).getDrained());
            assertThat(localInputs(stagingRoot)).hasSize(1);
            manager.releaseTaskExecutor(id);
            var released = awaitReleasedReport(reports, accepted.taskRequest(), TaskState.TASK_STATE_FINISHED);
            assertThat(released.getUsage().getObjectStoreReadBytes()).isGreaterThanOrEqualTo(2 * fileSize)
                    .isLessThanOrEqualTo(accepted.taskRequest().getResourceGrant().getObjectStoreReadBytes());
            assertThat(released.getExecutionDrainState()).isEqualTo(org.pangrova.protocol.v2.ExecutionDrainState
                    .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
            assertThat(manager.getExecutorHandle(id).orElse(0)).isZero();
            assertThat(localInputs(stagingRoot)).isEmpty();
        }
    }

    private static TaskReport awaitReleasedReport(List<org.pangrova.protocol.rest.task.TaskStatusReport> reports,
            TaskRequest request, TaskState state) {
        var found = new java.util.concurrent.atomic.AtomicReference<TaskReport>();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
            for (var report : reports) {
                if (!report.taskId().equals(request.getAttempt().getTaskId())) continue;
                byte[] encoded = report.canonicalTaskReport();
                if (encoded == null || encoded.length == 0) continue;
                var canonical = TaskReport.parseFrom(encoded);
                if (canonical.getState() == state && canonical.getExecutionDrainState()
                        != org.pangrova.protocol.v2.ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED) {
                    found.set(canonical);
                    return true;
                }
            }
            return false;
        });
        return found.get();
    }

    private static String submit(org.pangrova.worker.task.TaskManager manager, SubmitTaskRequest request) {
        manager.observeControlEpoch(request.taskRequest().getAttempt().getControlEpoch());
        return manager.submitTask(org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter
                .toTaskDescriptor(request), request);
    }

    private static List<Path> localInputs(Path root) throws Exception {
        try (var files = Files.walk(root)) {
            return files.filter(path -> path.getFileName().toString().equals("source.parquet")
                    && path.getParent().getFileName().toString().startsWith("copy-scan-")).toList();
        }
    }

    private static void readExactValues(ActualNativeTaskFixture runtime, TaskRequest request) throws Exception {
        int rows = 0;
        try (var allocator = new RootAllocator(ActualNativeTaskFixture.TASK_MEMORY_BYTES);
             var client = FlightClient.builder(allocator,
                     Location.forGrpcInsecure("127.0.0.1", runtime.resultPort())).build();
             var stream = client.getStream(new Ticket(
                     runtime.resultTicket(request).getBytes(StandardCharsets.UTF_8)))) {
            while (stream.next()) {
                var root = stream.getRoot();
                assertThat(root.getFieldVectors()).hasSize(2);
                for (int row = 0; row < root.getRowCount(); row++) {
                    assertThat(root.getVector(0).getObject(row)).isEqualTo(7L);
                    assertThat(root.getVector(1).getObject(row).toString()).isEqualTo("COPY exact Parquet value");
                    rows++;
                }
            }
        }
        assertThat(rows).isEqualTo(1);
    }

    static SubmitTaskRequest pendingTask(CopySourceReadHttp.Scope scope,
                                         CopySourceReadHttp.StagedSource proof, long grant) {
        String taskId = "native-copy-" + java.util.UUID.randomUUID();
        byte[] plan = readPlan(scope.stagingAttemptId());
        var request = TaskRequest.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                .setAttempt(TaskAttemptId.newBuilder().setQueryOrJobId(scope.executionId())
                        .setTaskId(taskId).setPlanEpoch(1).setFencingToken((1L << 32) | 1L)
                        .setControlEpoch(scope.controlEpoch()).setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                                .setOwnerId(scope.ownerId()).setOwnerIncarnation(scope.ownerIncarnation())
                                .setFencingToken(scope.ownerFencingToken())))
                .setTaskKind(TaskKind.TASK_KIND_READ).setSecurity(SecurityContext.newBuilder()
                        .setTenantId(scope.tenantId()).setPrincipal(scope.principal()))
                .setSubstraitPlan(ByteString.copyFrom(plan)).setSubstraitPlanChecksum(CanonicalTaskRequests.sha256(plan))
                .setResourceGrant(StageResourceGrant.newBuilder().setGrantId(taskId + ":grant")
                        .setCpuSlots(1).setNativeMemoryBytes(ActualNativeTaskFixture.TASK_MEMORY_BYTES)
                        .setShuffleBufferBytes(8L * 1024 * 1024).setObjectStoreReadBytes(grant))
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setDirectResultOutput(true))
                .setDeadlineEpochMs(scope.deadlineEpochMs())
                .addSplits(DataSplit.newBuilder().setSplitId("copy-input").setConnectorId("copy")
                        .setSourceId(scope.stagingAttemptId()).setDataObjectId(scope.canonicalSourceUrl())
                        .setOpaqueLocation(scope.canonicalSourceUrl()).setSourceSnapshot(proof.sourceVersionId())
                        .setLength(proof.contentSize()).setConnectorDetail(Any.pack(FileSplitDetail.newBuilder()
                                .setFileFormat(FileFormat.FILE_FORMAT_PARQUET).build()))).build();
        return SubmitTaskRequest.from(request);
    }

    private static byte[] readPlan(String sourceId) {
        var schema = NamedStruct.newBuilder().addNames("id").addNames("payload")
                .setStruct(Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                        .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(Type.newBuilder().setString(Type.String.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_NULLABLE))));
        var read = ReadRel.newBuilder().setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(sourceId));
        return Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setRead(read)).addNames("id").addNames("payload")))
                .build().toByteArray();
    }
}
