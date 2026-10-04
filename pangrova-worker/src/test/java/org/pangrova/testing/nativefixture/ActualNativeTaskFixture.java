package org.pangrova.testing.nativefixture;

import io.substrait.proto.Plan;
import io.substrait.proto.Type;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ResourceDrainEvidence;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.ResourceDrainEvidenceRequest;
import org.pangrova.protocol.v2.ResourceDrainScope;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.NodeMemoryPool;
import org.pangrova.resource.memory.RootMemoryPool;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap;
import org.pangrova.worker.bootstrap.phase.NativeRuntimeBootstrap;
import org.pangrova.worker.cache.NativeCacheConfig;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.worker.jni.NativeFunctionConfig;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.result.NativeResultTransport;
import org.pangrova.worker.result.ResultReadTickets;
import org.pangrova.worker.shuffle.NativeShuffleTransport;
import org.pangrova.worker.task.TransportRefs;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/** Actual packaged JNI runtime, admitted values tasks and live Native transports. */
public final class ActualNativeTaskFixture implements AutoCloseable {
    /** Tests sharing the process-wide JNI runtime must use this JUnit resource lock. */
    public static final String RUNTIME_LOCK = "pangrova-packaged-native-runtime";
    public static final long TASK_MEMORY_BYTES = 64L * 1024 * 1024;
    public static final long TOTAL_MEMORY_BYTES = 1024L * 1024 * 1024;

    private final MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_MEMORY_BYTES, 0);
    private final NodeMemoryPool nodePool = new RootMemoryPool("jni-fixture-root", TOTAL_MEMORY_BYTES)
            .createNodePool("jni-fixture-node", TOTAL_MEMORY_BYTES);
    private final WorkerComponentRefs nativeRefs = new WorkerComponentRefs();
    private final NativeTaskExecutor executor;
    private final NativeResultTransport results;
    private final NativeExchangeTransport exchange;
    private final TransportRefs transports;
    private final ResultReadTickets tickets;

    public ActualNativeTaskFixture() {
        if (NativeRuntime.isInitialized()) {
            throw new IllegalStateException("A different fixture still owns the Native runtime");
        }
        var config = TestShuffleServiceConfig.config(WorkerBootstrapConfig.withDefaults("actual-jni-fixture"));
        var endpoints = initializeRuntime(config);
        executor = endpoints.executor();
        results = endpoints.results();
        exchange = endpoints.exchange();
        tickets = endpoints.tickets();
        transports = new TransportRefs(new NativeShuffleTransport(executor, "127.0.0.1", ""), results, exchange);
    }

    private Endpoints initializeRuntime(WorkerBootstrapConfig config) {
        NativeTaskExecutor startingExecutor = null;
        NativeResultTransport startingResults = null;
        NativeExchangeTransport startingExchange = null;
        try {
            nativeRefs.setMemoryArbitrator(arbitrator);
            nativeRefs.setNodeMemoryPool(nodePool);
            NativeRuntimeBootstrap.initialize(config, nativeRefs, NativeCacheConfig.DISABLED);
            // This fixture owns core functions only, but still performs the real startup publication.
            NativeFunctionBootstrap.initialize(nativeRefs, NativeFunctionConfig.DISABLED);
            startingExecutor = new NativeTaskExecutor();
            var resultTickets = initializeTickets();
            startingResults = new NativeResultTransport(startingExecutor, "127.0.0.1");
            startingResults.startServer(0);
            startingExchange = new NativeExchangeTransport(startingExecutor);
            return new Endpoints(startingExecutor, startingResults, startingExchange, resultTickets);
        } catch (RuntimeException | Error failure) {
            closeAfterFailure(startingResults, failure);
            closeAfterFailure(startingExchange, failure);
            closeAfterFailure(startingExecutor, failure);
            closeAfterFailure(() -> NativeFunctionBootstrap.close(nativeRefs), failure);
            try {
                NativeRuntime.shutdownNativeRuntime();
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static ResultReadTickets initializeTickets() {
        byte[] key = new byte[32];
        new java.security.SecureRandom().nextBytes(key);
        NativeRuntime.nativeSetHmacKey(key, new byte[0]);
        return new ResultReadTickets(key);
    }

    private static void closeAfterFailure(AutoCloseable owner, Throwable failure) {
        if (owner == null) return;
        try {
            owner.close();
        } catch (Exception | Error cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    public NativeTaskExecutor executor() { return executor; }
    public MemoryArbitrator arbitrator() { return arbitrator; }
    public NodeMemoryPool nodePool() { return nodePool; }
    public TransportRefs transports() { return transports; }
    public int resultPort() { return results.boundPort(); }
    public String resultTicket(TaskRequest request) { return tickets.issue(request); }

    /** Uses the authoritative generated Substrait payload, not a synthetic plan decoder. */
    public TaskRequest valuesRequest(boolean resultOutput) {
        return valuesTaskRequest(resultOutput);
    }

    /** Builds real generated values bytes without acquiring a process-wide runtime. */
    public static TaskRequest valuesTaskRequest(boolean resultOutput) {
        return planTaskRequest(resultOutput, valuesPlan());
    }

    /** Uses an actual canonical relation plan under the same runtime authority. */
    public static TaskRequest planTaskRequest(boolean resultOutput, byte[] plan) {
        String id = UUID.randomUUID().toString();
        var request = CanonicalTaskRequestTestFixture.builder(
                        id, "actual-jni-" + id, 0, 0, TaskKind.TASK_KIND_READ, plan)
                .setResourceGrant(StageResourceGrant.newBuilder().setGrantId(id + ":grant")
                        .setCpuSlots(1).setNativeMemoryBytes(TASK_MEMORY_BYTES)
                        .setShuffleBufferBytes(8L * 1024 * 1024))
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setDirectResultOutput(resultOutput))
                .setDeadlineEpochMs(Instant.now().plusSeconds(60).toEpochMilli()).build();
        CanonicalTaskRequests.validateForAdmission(request);
        return request;
    }

    private static byte[] valuesPlan() {
        var plan = Plan.newBuilder();
        var root = plan.addRelationsBuilder().getRootBuilder().addNames("value");
        var read = root.getInputBuilder().getReadBuilder();
        read.getBaseSchemaBuilder().addNames("value").getStructBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .addTypesBuilder().getI64Builder().setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        read.getVirtualTableBuilder().addExpressionsBuilder().addFieldsBuilder().getLiteralBuilder().setI64(42);
        return plan.build().toByteArray();
    }

    public long createPending(TaskRequest request) {
        return executor.createTask(request.toByteArray());
    }

    public long createStarted(TaskRequest request) {
        long handle = createPending(request);
        executor.startTask(handle);
        return handle;
    }

    public TaskReport awaitFinished(long handle) throws Exception {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20))
                .until(() -> executor.isFinished(handle));
        return TaskReport.parseFrom(executor.getTaskStatus(handle));
    }

    /** Observes real live blocked drivers before allowing the actual consumer to resume. */
    public void awaitConsumerPressure(long handle) {
        org.awaitility.Awaitility.await().during(Duration.ofMillis(20)).atMost(Duration.ofSeconds(20))
                .until(() -> NativeTaskStats.parseFrom(executor.getTaskStats(handle))
                        .getConsumerBlockedDrivers() > 0);
    }

    /** Reads the real command response with exact attempt and security authority. */
    public ResourceDrainEvidence drainEvidence(long handle, TaskRequest request) throws Exception {
        var payload = ResourceDrainEvidenceRequest.newBuilder().setSecurity(request.getSecurity())
                .setResource(ResourceDrainScope.RESOURCE_DRAIN_SCOPE_RESULT_STREAM).build();
        byte[] command = NativeCommandBuilder.buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_GET_RESOURCE_DRAIN_EVIDENCE, request.getAttempt(), payload);
        var response = NativeCommandBuilder.requireSuccess(executor.executeCommand(handle, command), "actual drain");
        if (!response.hasPayload() || !response.getPayload().is(ResourceDrainEvidence.class)) {
            throw new IllegalStateException("Native did not return canonical resource drain evidence");
        }
        var evidence = response.getPayload().unpack(ResourceDrainEvidence.class);
        if (!evidence.getAttempt().equals(request.getAttempt())
                || !evidence.getSecurity().equals(request.getSecurity())) {
            throw new IllegalStateException("Native returned a different resource owner");
        }
        return evidence;
    }

    @Override
    public void close() {
        // Stop readers and their final buffers before joining handles. A failed
        // release must remain retryable rather than shutting down its runtime.
        stopTransports();
        executor.close();
        NativeFunctionBootstrap.close(nativeRefs);
        NativeRuntime.shutdownNativeRuntime();
        if (nodePool.getUsedBytes() != 0
                || arbitrator.getUsedMemory() != 0) {
            throw new IllegalStateException("Actual Native shutdown retained memory reservations");
        }
    }

    /** Quiesces actual server/readers before a test closes the shared executor. */
    public void stopTransports() {
        try {
            results.close();
        } catch (RuntimeException | Error failure) {
            closeAfterFailure(exchange, failure);
            throw failure;
        }
        exchange.close();
    }

    private record Endpoints(NativeTaskExecutor executor, NativeResultTransport results,
                             NativeExchangeTransport exchange, ResultReadTickets tickets) {}
}
