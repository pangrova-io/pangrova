package org.pangrova.worker.ai.invocation;

import org.pangrova.worker.ai.governance.AiInvocationObserver;

import org.pangrova.worker.ai.governance.AiInvocationAuthorizer;

import org.pangrova.spi.ai.AiCapabilityDescriptor;
import org.pangrova.spi.ai.AiModelIdentity;
import org.pangrova.spi.ai.AiInvocationRequest;
import org.pangrova.spi.ai.AiRequestConstraints;
import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.worker.ai.context.AiExecutionContext;
import org.pangrova.worker.ai.context.BoundAiInvocation;
import org.pangrova.worker.ai.provider.ConfiguredAiProviderRegistry;
import org.pangrova.worker.ai.provider.EnvironmentSecretResolver;

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
        registry = new ConfiguredAiProviderRegistry(config, new EnvironmentSecretResolver());
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
        config.put(prefix + "timeout-seconds", "30");
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
        return context(taskId, AiCapabilityDescriptor.Operation.TEXT_GENERATION, 8);
    }

    private AiExecutionContext context(String taskId, AiCapabilityDescriptor.Operation operation, long calls) {
        return contexts.computeIfAbsent(taskId, key -> {
            var request = taskRequest(key, operation, calls);
            AiInvocationService.registerTask(request);
            return AiExecutionContext.fromTaskRequest(request);
        });
    }

    private org.pangrova.protocol.v2.TaskRequest taskRequest(
            String taskId, AiCapabilityDescriptor.Operation operation, long calls) {
        var builder = org.pangrova.testing.nativefixture.CanonicalTaskRequestTestFixture.builder(
                taskId, "query-a", 0, 0, org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ, authorityPlan());
        builder.setAttempt(builder.getAttempt().toBuilder().setExecutionOwner(
                builder.getAttempt().getExecutionOwner().toBuilder().setOwnerIncarnation(incarnation)));
        builder.setSecurity(org.pangrova.protocol.v2.SecurityContext.newBuilder().setTenantId("tenant-a").setPrincipal("alice"))
                .setTraceContext(org.pangrova.protocol.v2.TraceContext.newBuilder().setTraceId("trace-a"))
                .setDeadlineEpochMs(Instant.now().plusSeconds(60).toEpochMilli())
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId(taskId + ":grant").setCpuSlots(2).setNativeMemoryBytes(1048576)
                        .setProviderConcurrency(2).setProviderItems(1024).setProviderAttempts(4096)
                        .setProviderRequestBytes(268435456).setProviderResponseBytes(1073741824));
        builder.addOperatorBindings(operatorBinding(binding(AiCapabilityDescriptor.Operation.TEXT_GENERATION,
                taskId, operation == AiCapabilityDescriptor.Operation.TEXT_GENERATION ? calls : 8)));
        builder.addOperatorBindings(operatorBinding(binding(AiCapabilityDescriptor.Operation.EMBEDDING,
                taskId, operation == AiCapabilityDescriptor.Operation.EMBEDDING ? calls : 8)));
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
        boolean embeds = binding.operation() == AiCapabilityDescriptor.Operation.EMBEDDING;
        var output = embeds ? org.pangrova.protocol.type.VectorLogicalType.create(384)
                : org.pangrova.protocol.v2.LogicalType.newBuilder().setCanonicalSql("VARCHAR")
                        .setSubstraitType(io.substrait.proto.Type.newBuilder().setString(
                                io.substrait.proto.Type.String.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE))).build();
        return org.pangrova.protocol.v2.OperatorBinding.newBuilder().setPlanNodeId(binding.invocationId())
                .setCapability(embeds ? org.pangrova.protocol.BuiltInCapabilityCatalog.aiEmbeddingPlanRef()
                        : org.pangrova.protocol.BuiltInCapabilityCatalog.aiTextGenerationPlanRef())
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

    BoundAiInvocation binding(AiCapabilityDescriptor.Operation operation, String taskId, long calls) {
        String name = operation == AiCapabilityDescriptor.Operation.EMBEDDING ? "embed" : "chat";
        var model = registry.models().stream().filter(value -> value.name().equals(name)).findFirst().orElseThrow();
        return new BoundAiInvocation("operator-" + name, name, model.version(), AiModelIdentity.signatureSha256(model),
                operation, "query-a:stage-a:" + taskId + ":operator-" + name,
                REQUEST_LIMITS, 2, calls, 32, 2097152, 8388608, 0,
                operation == AiCapabilityDescriptor.Operation.EMBEDDING ? 384 : 0);
    }

    <T> T invoke(AiCapabilityDescriptor.Operation operation, String taskId, long calls, Supplier<T> callback) {
        return AiExecutionContext.callWith(context(taskId, operation, calls), () ->
                AiExecutionContext.callWithBoundInvocation(binding(operation, taskId, calls), callback));
    }

    AiInvocationRequest request(AiCapabilityDescriptor.Operation operation, String taskId,
            AiCapabilityDescriptor.ExecutionMode mode, java.util.List<AiInvocationRequest.Input> inputs) {
        var binding = binding(operation, taskId, 8);
        var context = context(taskId);
        return new AiInvocationRequest("request-" + taskId, context.tenantId(), context.principal(),
                context.queryId(), context.traceId(), binding.modelId(), binding.modelVersion(),
                binding.modelSignatureSha256(), binding.modelVersion(), operation, mode,
                operation == AiCapabilityDescriptor.Operation.EMBEDDING
                        ? Optional.of(org.pangrova.spi.ai.embedding.EmbeddingPurpose.DOCUMENT) : Optional.empty(),
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
