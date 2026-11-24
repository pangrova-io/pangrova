package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.worker.governance.observer.AiInvocationObserver;
import org.pangrova.ai.worker.governance.AiInvocationAuthorizer;
import org.pangrova.ai.api.model.AiModelIdentity;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.capability.AiRequestConstraints;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.embedding.EmbeddingProvider;
import org.pangrova.ai.api.inference.InferenceProvider;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.AiTestTaskAuthority;
import org.pangrova.ai.worker.context.BoundAiInvocation;
import org.pangrova.ai.worker.provider.ConfiguredAiProviderRegistry;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Real production providers; the caller supplies the actual transport endpoint. */
final class GovernedAiTestModels implements AutoCloseable {
    static final String EMBEDDING_MODEL = "all-minilm:latest";
    static final String INFERENCE_MODEL = "tinyllama:latest";
    private final ConfiguredAiProviderRegistry registry;
    private final Map<String, AiExecutionContext> contexts = new ConcurrentHashMap<>();
    private final String incarnation = java.util.UUID.randomUUID().toString();
    private static final AiRequestConstraints REQUEST_LIMITS = new AiRequestConstraints(1000, 500, 262144, 1048576);

    GovernedAiTestModels(String endpoint) {
        Map<String, String> config = new LinkedHashMap<>();
        addModel(config, "chat", INFERENCE_MODEL, "inference", endpoint + "/v1/chat/completions");
        addModel(config, "embed", EMBEDDING_MODEL, "embedding", endpoint + "/v1/embeddings");
        config.put("models.embed.embedding-dimension", "384");
        config.put("models.embed.query-prefix", "query: ");
        config.put("models.embed.document-prefix", "doc: ");
        registry = new ConfiguredAiProviderRegistry(config, SecretResolverRegistry.discover(GovernedAiTestModels.class.getClassLoader()));
    }

    private static void addModel(Map<String, String> config, String name,
            String physicalModel, String capability, String endpoint) {
        String prefix = "models." + name + ".";
        config.put(prefix + "provider", "openai-compatible");
        config.put(prefix + "provider-model", physicalModel);
        config.put(prefix + "version", "test-v1");
        config.put(prefix + "endpoint", endpoint);
        config.put(prefix + "authentication", "none");
        config.put(prefix + "capabilities", capability);
        config.put(prefix + "max-batch-size", "8");
        config.put(prefix + "max-requests-per-second", "1000");
        config.put(prefix + "timeout-seconds", "120");
        config.put(prefix + "max-input-tokens", "1000");
        config.put(prefix + "max-output-tokens", "500");
        config.put(prefix + "max-media-bytes", "0");
        config.put(prefix + "max-concurrency", "2");
        config.put(prefix + "max-provider-items-per-task", "1024");
        config.put(prefix + "max-provider-attempts-per-task", "4096");
        config.put(prefix + "max-request-bytes", "262144");
        config.put(prefix + "max-response-bytes", "1048576");
    }

    InferenceProvider inference() { return registry.resolveInference("chat"); }
    EmbeddingProvider embedding() { return registry.resolveEmbedding("embed"); }

    AiInvocationService service(AiInvocationAuthorizer authorizer, AiInvocationObserver observer) {
        return new AiInvocationService(registry::resolveInference, registry::resolveEmbedding,
                registry::resolveReranking, authorizer, observer);
    }

    AiExecutionContext context(String taskId) {
        return context(taskId, AiOperation.TEXT_GENERATION, 8);
    }

    private AiExecutionContext context(String taskId, AiOperation operation, long calls) {
        return contexts.computeIfAbsent(taskId, key -> {
            var request = taskRequest(key, operation, calls);
            return AiTestTaskAuthority.register(request);
        });
    }

    private org.pangrova.protocol.v2.TaskRequest taskRequest(
            String taskId, AiOperation operation, long calls) {
        var builder = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                taskId, "query-a", 0, 0, org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ, authorityPlan());
        builder.setAttempt(builder.getAttempt().toBuilder().setExecutionOwner(
                builder.getAttempt().getExecutionOwner().toBuilder().setOwnerIncarnation(incarnation)));
        builder.setSecurity(org.pangrova.protocol.v2.SecurityContext.newBuilder().setTenantId("tenant-a").setPrincipal("alice"))
                .setTraceContext(org.pangrova.protocol.v2.TraceContext.newBuilder().setTraceId("trace-a"))
                .setDeadlineEpochMs(Instant.now().plusSeconds(180).toEpochMilli())
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId(taskId + ":grant").setCpuSlots(2).setNativeMemoryBytes(1048576)
                        .setProviderConcurrency(2).setProviderItems(1024).setProviderAttempts(4096)
                        .setProviderRequestBytes(268435456).setProviderResponseBytes(1073741824));
        builder.addOperatorBindings(operatorBinding(binding(AiOperation.TEXT_GENERATION,
                taskId, operation == AiOperation.TEXT_GENERATION ? calls : 8)));
        builder.addOperatorBindings(operatorBinding(binding(AiOperation.EMBEDDING,
                taskId, operation == AiOperation.EMBEDDING ? calls : 8)));
        return builder.build();
    }

    private static byte[] authorityPlan() {
        var schema = io.substrait.proto.NamedStruct.newBuilder().addNames("value")
                .setStruct(io.substrait.proto.Type.Struct.newBuilder().addTypes(io.substrait.proto.Type.newBuilder()
                        .setI64(io.substrait.proto.Type.I64.newBuilder()
                                .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED))));
        var read = io.substrait.proto.ReadRel.newBuilder().setBaseSchema(schema)
                .setVirtualTable(io.substrait.proto.ReadRel.VirtualTable.newBuilder().addValues(
                        io.substrait.proto.Expression.Literal.Struct.newBuilder().addFields(
                                io.substrait.proto.Expression.Literal.newBuilder().setI64(1))));
        return io.substrait.proto.Plan.newBuilder().addRelations(io.substrait.proto.PlanRel.newBuilder().setRoot(
                io.substrait.proto.RelRoot.newBuilder().addNames("value").setInput(
                        io.substrait.proto.Rel.newBuilder().setRead(read)))).build().toByteArray();
    }

    private static org.pangrova.protocol.v2.OperatorBinding operatorBinding(BoundAiInvocation binding) {
        boolean embeds = binding.operation() == AiOperation.EMBEDDING;
        var output = embeds ? org.pangrova.protocol.type.VectorLogicalType.create(384)
                : org.pangrova.protocol.v2.LogicalType.newBuilder().setCanonicalSql("VARCHAR")
                        .setSubstraitType(io.substrait.proto.Type.newBuilder().setString(
                                io.substrait.proto.Type.String.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE))).build();
        return org.pangrova.protocol.v2.OperatorBinding.newBuilder().setPlanNodeId(binding.invocationId())
                .setCapability(embeds ? org.pangrova.protocol.capability.BuiltInCapabilityCatalog.aiEmbeddingPlanRef()
                        : org.pangrova.protocol.capability.BuiltInCapabilityCatalog.aiTextGenerationPlanRef())
                .setModel(org.pangrova.protocol.v2.ModelBinding.newBuilder().setModel(
                        org.pangrova.protocol.v2.VersionedRef.newBuilder().setId(binding.modelId())
                                .setVersion(binding.modelVersion()).setSignatureSha256(
                                        com.google.protobuf.ByteString.copyFrom(binding.modelSignatureSha256())))
                        .setOutputType(output).setRequestLimits(org.pangrova.protocol.v2.ModelRequestLimits.newBuilder()
                                .setMaxInputTokens(1000).setMaxOutputTokens(500).setMaxRequestBytes(262144).setMaxResponseBytes(1048576)))
                .setExecutionPolicy(org.pangrova.protocol.v2.ExecutionPolicy.newBuilder().setIdempotencyKey(binding.idempotencyBase()))
                .setResources(org.pangrova.protocol.v2.OperatorResourceRequirement.newBuilder()
                        .setProviderConcurrency(binding.maxProviderConcurrency()).setProviderItems(binding.maxProviderItems())
                        .setProviderAttempts(binding.maxProviderAttempts()).setProviderRequestBytes(binding.maxProviderRequestBytes())
                        .setProviderResponseBytes(binding.maxProviderResponseBytes())).build();
    }

    BoundAiInvocation binding(AiOperation operation, String taskId, long calls) {
        String name = operation == AiOperation.EMBEDDING ? "embed" : "chat";
        var model = registry.models().stream().filter(value -> value.name().equals(name)).findFirst().orElseThrow();
        return new BoundAiInvocation("operator-" + name, name, model.version(), AiModelIdentity.signatureSha256(model),
                operation, "query-a:stage-a:" + taskId + ":operator-" + name,
                REQUEST_LIMITS, 2, calls, 32, 2097152, 8388608, 0,
                operation == AiOperation.EMBEDDING ? 384 : 0);
    }

    <T> T invoke(AiOperation operation, String taskId, long calls, Supplier<T> callback) {
        return AiExecutionContext.callWith(context(taskId, operation, calls), () ->
                AiExecutionContext.callWithBoundInvocation(binding(operation, taskId, calls), callback));
    }

    AiInvocationRequest request(AiOperation operation, String taskId,
            AiExecutionMode mode, java.util.List<AiInvocationRequest.Input> inputs) {
        var binding = binding(operation, taskId, 8);
        var context = context(taskId);
        return new AiInvocationRequest("request-" + taskId, context.tenantId(), context.principal(),
                context.queryId(), context.traceId(), binding.modelId(), binding.modelVersion(),
                binding.modelSignatureSha256(), binding.modelVersion(), operation, mode,
                operation == AiOperation.EMBEDDING
                        ? Optional.of(org.pangrova.ai.api.enums.EmbeddingPurpose.DOCUMENT) : Optional.empty(),
                inputs, GenerationParameters.DEFAULTS, context.deadline().orElseThrow(),
                binding.idempotencyBase() + ":" + "0".repeat(64), Map.of("task_id", taskId,
                        "task_attempt_base64", java.util.Base64.getEncoder().encodeToString(context.taskAttempt().toByteArray())));
    }

    @Override
    public void close() {
        contexts.values().forEach(context -> AiInvocationService.releaseTask(context.tenantId(), context.taskAttempt()));
        registry.close();
    }
}
