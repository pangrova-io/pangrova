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
package org.pangrova.ai.worker.function;

import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.InferenceProvider;
import org.pangrova.ai.api.invocation.InferenceResult;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.function.api.scalar.ScalarFunction;
import org.pangrova.protocol.function.GovernedBindingProjection;
import org.pangrova.protocol.function.GovernedScalarValue;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.ModelRequestLimits;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.AiTestTaskAuthority;
import org.pangrova.ai.worker.invocation.AiInvocationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.ai.worker.blob.GovernedBlobStore;
import org.pangrova.ai.worker.blob.HttpsBlobImporter;
import org.pangrova.ai.worker.blob.BlobTestStores;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AiFunctionFactoryTest {

    private StubInferenceProvider stubProvider;
    private AiFunctionFactory factory;

    @TempDir
    Path temporaryDirectory;

    @BeforeEach
    void setUp() {
        stubProvider = new StubInferenceProvider();
        GovernedBlobStore store = BlobTestStores.create(
                temporaryDirectory, 1024, Set.of("image/png", "text/plain"));
        factory = new AiFunctionFactory(
                name -> "test-model".equals(name) ? stubProvider : null,
                name -> null,
                name -> null,
                store,
                new HttpsBlobImporter(
                        store, new HttpsBlobImporter.Settings(
                                Set.of(), Set.of("image/png", "text/plain"),
                                Duration.ofSeconds(1), Duration.ofSeconds(1), 1024, 1024,
                                new HttpsBlobImporter.PoolSettings(
                                        8, 4, Duration.ofMinutes(1),
                                        Duration.ofSeconds(10)))));
    }

    @Test
    void productionFactorySelectsExplicitLocalStorage() throws Exception {
        Path objects = temporaryDirectory.resolve("local-objects");
        try (var production = new AiFunctionFactory()) {
            var functions = production.getScalarFunctions(Map.of(
                    "file.storage-backend", "local",
                    "file.storage.local.root", objects.toString(),
                    "file.storage-staging-root", temporaryDirectory.resolve("local-drafts").toString(),
                    "file.capability-secret-reference", "test://file-capability"));
            assertTrue(java.nio.file.Files.isDirectory(objects));
            var fromBytes = functions.stream()
                    .filter(function -> function.getMetadata().name().equals("file_from_bytes"))
                    .findFirst().orElseThrow();
            byte[] content = {1, 2, 3};
            ColumnValue result = AiTestTaskAuthority.callWithRegisteredTask("tenant", "principal",
                    java.time.Instant.now().plusSeconds(30).toEpochMilli(), 1024,
                    () -> fromBytes.invoke(List.of(new ColumnValue.BytesValue(content),
                            new ColumnValue.StringValue("text/plain"))));
            var row = assertInstanceOf(ColumnValue.RowValue.class, result);
            assertEquals(3L, row.fields().get(2));
            assertEquals(java.util.HexFormat.of().formatHex(
                    java.security.MessageDigest.getInstance("SHA-256").digest(content)), row.fields().get(3));
            try (var files = java.nio.file.Files.walk(objects)) {
                var published = files.filter(java.nio.file.Files::isRegularFile).toList();
                assertEquals(1, published.size());
                assertArrayEquals(content, java.nio.file.Files.readAllBytes(published.get(0)));
            }
        }
    }

    @Test
    void localRootIsRequiredAndUnknownBackendCannotSelectADefault() throws Exception {
        Path drafts = temporaryDirectory.resolve("invalid-drafts");
        try (var production = new AiFunctionFactory()) {
            var missingRoot = assertThrows(IllegalArgumentException.class,
                    () -> production.getScalarFunctions(Map.of(
                            "file.storage-backend", "local",
                            "file.storage-staging-root", drafts.toString(),
                            "file.capability-secret-reference", "test://file-capability")));
            assertTrue(missingRoot.getMessage().contains("file.storage.local.root is required"));
            var unsupported = assertThrows(IllegalArgumentException.class,
                    () -> production.getScalarFunctions(Map.of(
                            "file.storage-backend", "oss",
                            "file.storage-staging-root", drafts.toString(),
                            "file.capability-secret-reference", "test://file-capability")));
            assertTrue(unsupported.getMessage().contains("Unsupported FILE storage backend"));
            assertFalse(java.nio.file.Files.exists(drafts));
        }
    }

    @Test
    void assemblyCleanupPreservesPrimaryErrorAndAttemptsEveryOwner() {
        var primary = new AssertionError("assembly failed");
        var cleanup = new AssertionError("cleanup failed");
        var closed = new java.util.concurrent.atomic.AtomicInteger();
        AiFunctionCatalog.closeAfterFailure(primary,
                () -> { throw cleanup; },
                () -> { throw primary; },
                closed::incrementAndGet);
        assertArrayEquals(new Throwable[] {cleanup}, primary.getSuppressed());
        assertEquals(1, closed.get());
    }

    @Test
    void factoryPublishesOnlyFunctionsWithExecutableWorkerBindings() {
        List<ScalarFunction> functions = factory.getScalarFunctions(Map.of());
        assertEquals(16, functions.size());
        List<String> names = functions.stream()
                .map(f -> f.getMetadata().name())
                .toList();
        assertTrue(names.contains("ai_complete"), "missing ai_complete");
        assertTrue(names.contains("ai_summarize"), "missing ai_summarize");
        assertTrue(names.contains("ai_classify"), "missing ai_classify");
        assertTrue(names.contains("ai_filter"), "missing ai_filter");
        assertTrue(names.contains("ai_embed"), "missing ai_embed");
        assertTrue(names.contains("ai_extract"), "missing ai_extract");
        assertTrue(names.contains("ai_guard"), "missing ai_guard");
        assertTrue(names.contains("ai_predict"), "missing ai_predict");
        assertTrue(names.contains("ai_translate"), "missing ai_translate");
        assertTrue(names.contains(
                AiRerankScoreFunction.NAME), "missing governed rerank score");
        assertTrue(names.contains("file_from_url"));
        assertTrue(names.contains("file_from_bytes"));
    }

    @Test
    void incompleteFileStorageConfigFailsClosed() {
        var discovered = new AiFunctionFactory();

        var failure = assertThrows(IllegalArgumentException.class,
                () -> discovered.getScalarFunctions(Map.of("file.storage-backend", "s3")));

        assertTrue(failure.getMessage()
                .contains("file.capability-secret-reference is required"));
    }

    @Test
    void aiCompleteReturnsGeneratedText() {
        var fn = new AiCompletionFunction(name -> stubProvider);
        stubProvider.setResponse("Hello world");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("Say hello")));
        assertInstanceOf(ColumnValue.StringValue.class, result);
        assertEquals("Hello world", ((ColumnValue.StringValue) result).value());
    }

    @Test
    void factoryFunctionRequiresAndConsumesExactNativeBinding() {
        ScalarFunction function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name()
                        .equals("ai_complete"))
                .findFirst()
                .orElseThrow();
        stubProvider.setResponse("bound answer");

        ColumnValue result = withAdmittedTask(task -> function.invoke(governedArguments(
                task, org.pangrova.protocol.capability.BuiltInCapabilityCatalog.aiTextGenerationPlanRef())));

        assertEquals(
                "bound answer", ((ColumnValue.StringValue) result).value());
    }

    @Test
    void factoryFunctionRejectsCapabilitySignatureTampering() {
        ScalarFunction function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name()
                        .equals("ai_complete"))
                .findFirst()
                .orElseThrow();
        var tampered = org.pangrova.protocol.capability.BuiltInCapabilityCatalog
                .aiTextGenerationPlanRef().toBuilder()
                .setSignatureSha256(com.google.protobuf.ByteString.copyFrom(
                        new byte[32]))
                .build();

        assertThrows(IllegalArgumentException.class, () ->
                withAdmittedTask(task -> function.invoke(governedArguments(task, tampered))));
    }

    @Test
    void factoryFunctionRejectsMixedBatchResourceBindings() {
        ScalarFunction function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name()
                        .equals("ai_complete"))
                .findFirst()
                .orElseThrow();
        assertThrows(IllegalArgumentException.class, () -> withAdmittedTask(task -> {
            List<ColumnValue> first = governedArguments(
                    task, org.pangrova.protocol.capability.BuiltInCapabilityCatalog.aiTextGenerationPlanRef());
            List<ColumnValue> changed = new java.util.ArrayList<>(first);
            changed.set(2 + FunctionBindingDeclarations.governedLayout("AI_MODEL")
                    .ordinal("provider_items"), new ColumnValue.StringValue("1"));
            return ((org.pangrova.function.api.scalar.BatchableScalarFunction) function)
                    .invokeBatch(List.of(first, changed));
        }));
    }

    @Test
    void aiCompleteReturnsNullOnNullInput() {
        var fn = new AiCompletionFunction(name -> stubProvider);

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.NullValue()));
        assertInstanceOf(ColumnValue.NullValue.class, result);
    }

    @Test
    void aiClassifyReturnsLabel() {
        var fn = new AiClassifyFunction(name -> stubProvider);
        stubProvider.setResponse("positive");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("This product is great!"),
                new ColumnValue.StringValue("positive,negative,neutral")));
        assertInstanceOf(ColumnValue.StringValue.class, result);
        assertEquals("positive", ((ColumnValue.StringValue) result).value());
    }

    @Test
    void aiSummarizeReturnsText() {
        var fn = new AiSummarizeFunction(name -> stubProvider);
        stubProvider.setResponse("Summary of the text.");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("A very long article...")));
        assertInstanceOf(ColumnValue.StringValue.class, result);
        assertEquals("Summary of the text.", ((ColumnValue.StringValue) result).value());
    }

    @Test
    void aiFilterReturnsBooleanTrue() {
        var fn = new AiFilterFunction(name -> stubProvider);
        stubProvider.setResponse("true");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("The product broke after 2 days"),
                new ColumnValue.StringValue("mentions product defect")));
        assertInstanceOf(ColumnValue.BooleanValue.class, result);
        assertTrue(((ColumnValue.BooleanValue) result).value());
    }

    @Test
    void aiFilterReturnsBooleanFalse() {
        var fn = new AiFilterFunction(name -> stubProvider);
        stubProvider.setResponse("false");

        ColumnValue result = fn.invoke(List.of(
                new ColumnValue.StringValue("test-model"),
                new ColumnValue.StringValue("Great product!"),
                new ColumnValue.StringValue("mentions product defect")));
        assertInstanceOf(ColumnValue.BooleanValue.class, result);
        assertFalse(((ColumnValue.BooleanValue) result).value());
    }

    @Test
    void missingProviderThrows() {
        var fn = new AiCompletionFunction(name -> null);

        assertThrows(IllegalStateException.class, () ->
                fn.invoke(List.of(
                        new ColumnValue.StringValue("unknown"),
                        new ColumnValue.StringValue("prompt"))));
    }

    @Test
    void factoryNameIsCorrect() {
        assertEquals("ai-inference", factory.getName());
    }

    private static <T> T withAdmittedTask(java.util.function.Function<TaskRequest, T> action) {
        String taskId = java.util.UUID.randomUUID().toString();
        var grant = StageResourceGrant.newBuilder()
                .setGrantId(taskId + ":grant").setCpuSlots(1).setNativeMemoryBytes(1_048_576)
                .setProviderConcurrency(2).setProviderItems(1024).setProviderAttempts(4096)
                .setProviderRequestBytes(1_024_000).setProviderResponseBytes(1_000_000).build();
        var binding = OperatorBinding.newBuilder().setPlanNodeId("ai-invoke-1")
                .setCapability(org.pangrova.protocol.capability.BuiltInCapabilityCatalog.aiTextGenerationPlanRef())
                .setModel(ModelBinding.newBuilder().setModel(VersionedRef.newBuilder()
                        .setId("test-model").setVersion("v1")
                        .setSignatureSha256(com.google.protobuf.ByteString.copyFrom(new byte[32])))
                        .setOutputType(LogicalType.newBuilder().setCanonicalSql("VARCHAR")
                                .setSubstraitType(io.substrait.proto.Type.newBuilder().setString(
                                        io.substrait.proto.Type.String.newBuilder().setNullability(
                                                io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE))))
                        .setRequestLimits(ModelRequestLimits.newBuilder()
                                .setMaxInputTokens(1024).setMaxOutputTokens(512)
                                .setMaxRequestBytes(1_024_000).setMaxResponseBytes(1_000_000)))
                .setExecutionPolicy(ExecutionPolicy.newBuilder()
                        .setIdempotencyKey("query-1:stage-1:" + taskId + ":ai-invoke-1"))
                .setResources(OperatorResourceRequirement.newBuilder()
                        .setProviderConcurrency(2).setProviderItems(1024).setProviderAttempts(4096)
                        .setProviderRequestBytes(1_024_000).setProviderResponseBytes(1_000_000))
                .build();
        var task = AiTestTaskAuthority.request(new AiTestTaskAuthority.Identity(
                "tenant", "principal", "query-1", taskId, "trace", java.time.Instant.now()
                        .plusSeconds(30).toEpochMilli()), grant).toBuilder()
                .addOperatorBindings(binding).build();
        AiInvocationService.registerTask(task);
        var context = AiExecutionContext.fromTaskRequest(task);
        try {
            return AiExecutionContext.callWith(context, () -> action.apply(task));
        } finally {
            AiInvocationService.releaseTask(context.tenantId(), context.taskAttempt());
        }
    }

    private static List<ColumnValue> governedArguments(TaskRequest task, CapabilityRef capability) {
        var layout = FunctionBindingDeclarations.governedLayout("AI_MODEL");
        var authority = GovernedBindingProjection.values(layout.id(),
                task.getOperatorBindings(0).toBuilder().setCapability(capability).build());
        var arguments = new java.util.ArrayList<ColumnValue>();
        arguments.add(new ColumnValue.StringValue("test-model"));
        arguments.add(new ColumnValue.StringValue("Say hello"));
        for (var declared : layout.arguments()) {
            arguments.add(new ColumnValue.StringValue(encode(authority.get(declared.name()))));
        }
        return List.copyOf(arguments);
    }

    private static String encode(GovernedScalarValue value) {
        if (value instanceof GovernedScalarValue.Text text) return text.value();
        if (value instanceof GovernedScalarValue.Signature signature) {
            return java.util.Base64.getEncoder().encodeToString(signature.value().toByteArray());
        }
        if (value instanceof GovernedScalarValue.Unsigned unsigned) return Long.toString(unsigned.value());
        if (value instanceof GovernedScalarValue.Flag flag) return Boolean.toString(flag.value());
        if (value instanceof GovernedScalarValue.Floating floating) return Double.toString(floating.value());
        throw new IllegalArgumentException("Unsupported declared authority value");
    }

    /**
     * Simple stub inference provider for testing.
     */
    private static class StubInferenceProvider implements InferenceProvider {
        private String response = "";

        void setResponse(String response) {
            this.response = response;
        }

        @Override
        public InferenceResult complete(String prompt, GenerationParameters parameters) {
            return InferenceResult.of(response);
        }

        @Override
        public List<InferenceResult> completeBatch(List<String> prompts, GenerationParameters parameters) {
            return prompts.stream().map(p -> InferenceResult.of(response)).toList();
        }

        @Override
        public String modelName() {
            return "test-model";
        }

        @Override
        public int maxBatchSize() {
            return 10;
        }
    }
}
