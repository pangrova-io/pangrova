package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.invocation.PhysicalAttempt;

import com.google.protobuf.ByteString;
import io.substrait.proto.Type;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.ModelRequestLimits;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.capability.AiRequestConstraints;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.ai.api.enums.EmbeddingPurpose;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.api.invocation.AiProviderCallContext;
import org.pangrova.ai.worker.context.AiTestTaskAuthority;
import org.pangrova.ai.worker.context.BoundAiInvocation;
import org.pangrova.ai.worker.context.GovernedAiBindingProjector;

import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/** Actual canonical budget/admission for transport component tests, not model/SQL/JNI acceptance. */
public final class AiTransportTestAdmission implements AutoCloseable {
    private final AiInvocationBudgetManager manager = new AiInvocationBudgetManager();
    private final AiExecutionContext context;
    private final BoundAiInvocation binding;
    private final AiPhysicalInvocationAdmission admission;

    public AiTransportTestAdmission(AiOperation operation, int dimension,
            int items, int providerRequests, Limits limits, Instant deadline) {
        String taskId = UUID.randomUUID().toString();
        var operator = operator(operation, dimension, limits);
        var task = AiTestTaskAuthority.request(new AiTestTaskAuthority.Identity(
                "tenant", "alice", "query", taskId, "trace", deadline.toEpochMilli()),
                StageResourceGrant.newBuilder().setGrantId(taskId + ":grant").setCpuSlots(1)
                        .setNativeMemoryBytes(1048576).setProviderConcurrency(limits.concurrency())
                        .setProviderItems(limits.items()).setProviderAttempts(limits.attempts())
                        .setProviderRequestBytes(limits.requestBytes()).setProviderResponseBytes(limits.responseBytes())
                        .setMediaBytes(limits.mediaBytes()).build()).toBuilder().addOperatorBindings(operator).build();
        context = AiTestTaskAuthority.context(task);
        binding = GovernedAiBindingProjector.fromAdmitted(operator);
        manager.registerTask(context, task.getOperatorBindingsList());
        var capability = capability(operation, limits);
        try {
            admission = AiExecutionContext.callWith(context, () -> AiExecutionContext.callWithBoundInvocation(binding,
                    () -> manager.acquire(context, request(operation, items, deadline), capability,
                            items, providerRequests, Math.min(providerRequests, limits.concurrency()), 0)));
        } catch (RuntimeException failure) {
            manager.releaseTask(context.tenantId(), context.taskAttempt());
            throw failure;
        }
    }

    public static AiTransportTestAdmission embedding(int dimension, int items, int requests) {
        return new AiTransportTestAdmission(AiOperation.EMBEDDING, dimension,
                items, requests, Limits.defaults(), Instant.now().plusSeconds(30));
    }

    public static <T> T inference(Supplier<T> callback) {
        try (var budget = new AiTransportTestAdmission(AiOperation.TEXT_GENERATION, 0,
                1, 1, Limits.defaults(), Instant.now().plusSeconds(30))) {
            return budget.call("transport-inference", callback);
        }
    }

    public <T> T call(String idempotencyKey, Supplier<T> callback) {
        return AiExecutionContext.callWith(context, () -> AiExecutionContext.callWithBoundInvocation(binding,
                () -> AiProviderCallContext.callWith(idempotencyKey, context.deadline().orElseThrow(),
                        admission::openAttempt, callback)));
    }

    public PhysicalAttempt openAttempt(long bytes) { return admission.openAttempt(bytes); }
    public ResourceUsage usage() { return manager.usage(context.tenantId(), context.taskAttempt()); }
    public void cancel() { manager.cancelTask(context.tenantId(), context.taskAttempt()); }
    public void terminate() { manager.releaseTask(context.tenantId(), context.taskAttempt()); }

    @Override public void close() {
        try { admission.close(); }
        finally { manager.releaseTask(context.tenantId(), context.taskAttempt()); }
    }

    private AiInvocationRequest request(AiOperation operation, int items, Instant deadline) {
        return new AiInvocationRequest("request", context.tenantId(), context.principal(), context.queryId(),
                context.traceId(), binding.modelId(), binding.modelVersion(), binding.modelSignatureSha256(), "v1",
                operation, items == 1 ? AiExecutionMode.SCALAR
                        : AiExecutionMode.BATCH,
                operation == AiOperation.EMBEDDING ? Optional.of(EmbeddingPurpose.RAW) : Optional.empty(),
                inputs(operation, items), GenerationParameters.DEFAULTS, deadline,
                binding.idempotencyBase() + ":" + "0".repeat(64), Map.of("task_id", context.taskId(),
                        "task_attempt_base64", Base64.getEncoder().encodeToString(context.taskAttempt().toByteArray())));
    }

    private static AiCapabilityDescriptor capability(AiOperation operation, Limits limits) {
        return new AiCapabilityDescriptor("transport", "v1", operation,
                List.of(new AiCapabilityDescriptor.Signature(
                        resultType(operation), List.of("VARCHAR"))),
                Set.of(AiExecutionMode.SCALAR, AiExecutionMode.BATCH),
                Set.of(Modality.TEXT), 128,
                new AiCapabilityDescriptor.ResourceLimits(limits.constraints(), limits.mediaBytes(), limits.concurrency(),
                        limits.items(), limits.attempts()), false,
                new AiCapabilityDescriptor.RetryPolicy(4, Duration.ZERO, Duration.ZERO, Set.of()), Set.of());
    }

    private static OperatorBinding operator(AiOperation operation, int dimension, Limits limits) {
        var capability = switch (operation) {
            case EMBEDDING -> BuiltInCapabilityCatalog.aiEmbeddingPlanRef();
            case TEXT_GENERATION -> BuiltInCapabilityCatalog.aiTextGenerationPlanRef();
            case MULTIMODAL_GENERATION -> BuiltInCapabilityCatalog.aiMultimodalGenerationPlanRef();
            case RERANKING -> BuiltInCapabilityCatalog.crossEncoderRerankPlanRef();
        };
        var output = outputType(operation, dimension);
        var constraints = limits.constraints();
        return OperatorBinding.newBuilder().setPlanNodeId("transport").setCapability(capability)
                .setExecutionPolicy(ExecutionPolicy.newBuilder().setIdempotencyKey("task:transport"))
                .setModel(ModelBinding.newBuilder().setModel(VersionedRef.newBuilder().setId("transport-model")
                        .setVersion("v1").setSignatureSha256(ByteString.copyFrom(new byte[32])))
                        .setOutputType(output).setRequestLimits(ModelRequestLimits.newBuilder()
                                .setMaxInputTokens(constraints.maxInputTokens()).setMaxOutputTokens(constraints.maxOutputTokens())
                                .setMaxRequestBytes(constraints.maxRequestBytes()).setMaxResponseBytes(constraints.maxResponseBytes())))
                .setResources(OperatorResourceRequirement.newBuilder().setProviderConcurrency(limits.concurrency())
                        .setProviderItems(limits.items()).setProviderAttempts(limits.attempts())
                        .setProviderRequestBytes(limits.requestBytes()).setProviderResponseBytes(limits.responseBytes())
                        .setMediaBytes(limits.mediaBytes())).build();
    }

    private static LogicalType outputType(AiOperation operation, int dimension) {
        if (operation == AiOperation.EMBEDDING) return VectorLogicalType.create(dimension);
        if (operation == AiOperation.RERANKING) {
            return LogicalType.newBuilder().setCanonicalSql("DOUBLE").setSubstraitType(Type.newBuilder()
                    .setFp64(Type.FP64.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE))).build();
        }
        return LogicalType.newBuilder().setCanonicalSql("VARCHAR").setSubstraitType(Type.newBuilder().setString(
                Type.String.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE))).build();
    }

    private static List<AiInvocationRequest.Input> inputs(AiOperation operation, int items) {
        return java.util.stream.IntStream.range(0, items).<AiInvocationRequest.Input>mapToObj(index ->
                operation == AiOperation.RERANKING
                        ? new AiInvocationRequest.RerankInput("query", "document-" + index)
                        : new AiInvocationRequest.TextInput("transport-" + index)).toList();
    }

    private static String resultType(AiOperation operation) {
        if (operation == AiOperation.EMBEDDING) return "VECTOR";
        return operation == AiOperation.RERANKING ? "DOUBLE" : "VARCHAR";
    }

    boolean hasQueuedAttempts() { return manager.taskBudget(context).concurrency.hasQueuedThreads(); }

    public record Limits(int concurrency, long items, long attempts, long requestBytes, long responseBytes,
            long mediaBytes, AiRequestConstraints constraints) {
        public static Limits defaults() {
            return new Limits(2, 1024, 4096, 268435456, 1073741824, 1048576,
                    new AiRequestConstraints(8192, 4096, 262144, 1048576));
        }
    }
}
