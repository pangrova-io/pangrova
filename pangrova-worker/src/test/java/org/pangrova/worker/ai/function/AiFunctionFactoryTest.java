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
package org.pangrova.worker.ai.function;

import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.ai.inference.InferenceResult;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.ScalarFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.worker.ai.blob.GovernedBlobStore;
import org.pangrova.worker.ai.blob.HttpsBlobImporter;
import org.pangrova.worker.ai.blob.BlobTestStores;
import org.pangrova.worker.ai.context.AiExecutionContext;
import org.pangrova.worker.ai.context.AiTestTaskAuthority;
import org.pangrova.worker.ai.invocation.AiInvocationService;
import org.pangrova.protocol.function.GovernedBindingProjection;
import org.pangrova.protocol.function.GovernedScalarValue;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.v2.TaskRequest;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AiFunctionFactoryTest {

    private StubInferenceProvider stubProvider;
    private AiFunctionFactory factory;
    private TaskRequest admittedTask;
    private GovernedBlobStore store;
    private HttpsBlobImporter importer;

    @TempDir
    Path temporaryDirectory;

    @BeforeEach
    void setUp() {
        stubProvider = new StubInferenceProvider();
        store = BlobTestStores.create(
                temporaryDirectory, 1024, Set.of("image/png", "text/plain"));
        importer = new HttpsBlobImporter(
                store, new HttpsBlobImporter.Settings(
                        Set.of(), Set.of("image/png", "text/plain"),
                        Duration.ofSeconds(1), Duration.ofSeconds(1), 1024, 1024,
                        new HttpsBlobImporter.PoolSettings(8, 4, Duration.ofMinutes(1), Duration.ofSeconds(10))));
        factory = new AiFunctionFactory(
                name -> "test-model".equals(name) ? stubProvider : null,
                name -> null,
                name -> null,
                store, importer);
        admittedTask = admittedTask();
        AiInvocationService.registerTask(admittedTask);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            AiInvocationService.releaseTask(admittedTask.getSecurity().getTenantId(), admittedTask.getAttempt());
        } finally {
            closeTestResources();
        }
    }

    private void closeTestResources() throws Exception {
        Exception failure = null;
        for (AutoCloseable resource : List.of(factory, importer, store)) {
            try {
                resource.close();
            } catch (Exception cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
        }
        if (failure != null) throw failure;
    }

    @Test
    void factoryFunctionRejectsMissingTaskAuthorityBeforeProviderCall() {
        var function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name().equals("ai_complete"))
                .findFirst().orElseThrow();
        var failure = assertThrows(org.pangrova.spi.ai.AiInvocationException.class,
                () -> function.invoke(governedArguments()));
        assertEquals(org.pangrova.spi.ai.AiInvocationException.Category.AUTHORIZATION, failure.category());
        assertEquals(0, stubProvider.calls);
    }

    @Test
    void factoryFunctionRejectsArgumentsBeyondEveryDeclaredSignature() {
        var function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name().equals("ai_complete"))
                .findFirst().orElseThrow();
        var arguments = new ArrayList<>(governedArguments());
        int largestSignature = function.getArgumentSignatures().stream()
                .mapToInt(List::size).max().orElseThrow();
        // A single extra value can select a legitimate public overload. Exceed
        // every real signature to test arity, not a shifted binding's authority.
        while (arguments.size() <= largestSignature) {
            arguments.add(new ColumnValue.StringValue("extra"));
        }
        var failure = assertThrows(IllegalArgumentException.class, () -> AiExecutionContext.callWith(
                AiExecutionContext.fromTaskRequest(admittedTask), () -> function.invoke(arguments)));
        assertTrue(failure.getMessage().contains("exact declared public and binding arguments"));
        assertEquals(0, stubProvider.calls);
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

        ColumnValue result = AiExecutionContext.callWith(AiExecutionContext.fromTaskRequest(admittedTask),
                () -> function.invoke(governedArguments()));

        assertEquals(
                "bound answer", ((ColumnValue.StringValue) result).value());
        assertEquals(1, stubProvider.calls);
    }

    @Test
    void factoryFunctionRejectsCapabilitySignatureTampering() {
        ScalarFunction function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name()
                        .equals("ai_complete"))
                .findFirst()
                .orElseThrow();
        var tampered = new ArrayList<>(governedArguments());
        tampered.set(2 + FunctionBindingDeclarations.governedLayout("AI_MODEL").ordinal("capability_signature"),
                new ColumnValue.StringValue(Base64.getEncoder().encodeToString(new byte[32])));

        var failure = assertThrows(IllegalArgumentException.class, () -> AiExecutionContext.callWith(
                AiExecutionContext.fromTaskRequest(admittedTask), () -> function.invoke(tampered)));
        assertTrue(failure.getMessage().contains("capability_signature"));
        assertEquals(0, stubProvider.calls);
    }

    @Test
    void factoryFunctionRejectsMixedBatchResourceBindings() {
        ScalarFunction function = factory.getScalarFunctions(Map.of()).stream()
                .filter(candidate -> candidate.getMetadata().name()
                        .equals("ai_complete"))
                .findFirst()
                .orElseThrow();
        List<ColumnValue> first = governedArguments();
        List<ColumnValue> changed = new java.util.ArrayList<>(first);
        changed.set(2 + FunctionBindingDeclarations.governedLayout("AI_MODEL").ordinal("provider_concurrency"),
                new ColumnValue.StringValue("1"));

        var failure = assertThrows(IllegalArgumentException.class, () -> AiExecutionContext.callWith(
                AiExecutionContext.fromTaskRequest(admittedTask), () ->
                ((org.pangrova.spi.function.BatchableScalarFunction) function)
                        .invokeBatch(List.of(first, changed))));
        assertTrue(failure.getMessage().contains("provider_concurrency"));
        assertEquals(0, stubProvider.calls);
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

    private List<ColumnValue> governedArguments() {
        var layout = FunctionBindingDeclarations.governedLayout("AI_MODEL");
        var authority = GovernedBindingProjection.values(layout.id(), admittedTask.getOperatorBindings(0));
        List<ColumnValue> arguments = new ArrayList<>(List.of(
                new ColumnValue.StringValue("test-model"), new ColumnValue.StringValue("Say hello")));
        layout.arguments().forEach(argument -> arguments.add(
                new ColumnValue.StringValue(encode(authority.get(argument.name())))));
        return List.copyOf(arguments);
    }

    private static String encode(GovernedScalarValue value) {
        if (value instanceof GovernedScalarValue.Text text) return text.value();
        if (value instanceof GovernedScalarValue.Signature signature) {
            return Base64.getEncoder().encodeToString(signature.value().toByteArray());
        }
        if (value instanceof GovernedScalarValue.Unsigned unsigned) return Long.toString(unsigned.value());
        if (value instanceof GovernedScalarValue.Flag flag) return Boolean.toString(flag.value());
        if (value instanceof GovernedScalarValue.Floating floating) return Double.toString(floating.value());
        throw new IllegalArgumentException("Unsupported declared authority value");
    }

    private static TaskRequest admittedTask() {
        String id = UUID.randomUUID().toString();
        var grant = org.pangrova.protocol.v2.StageResourceGrant.newBuilder().setGrantId(id + ":grant")
                .setCpuSlots(1).setNativeMemoryBytes(1048576).setProviderConcurrency(2).setProviderItems(1024)
                .setProviderAttempts(4096).setProviderRequestBytes(268435456).setProviderResponseBytes(1073741824);
        var binding = org.pangrova.protocol.v2.OperatorBinding.newBuilder().setPlanNodeId("ai-invoke-1")
                .setCapability(org.pangrova.protocol.BuiltInCapabilityCatalog.aiTextGenerationPlanRef())
                .setExecutionPolicy(org.pangrova.protocol.v2.ExecutionPolicy.newBuilder().setIdempotencyKey(id + ":ai-invoke-1"))
                .setModel(org.pangrova.protocol.v2.ModelBinding.newBuilder().setModel(
                        org.pangrova.protocol.v2.VersionedRef.newBuilder().setId("test-model").setVersion("v1")
                                .setSignatureSha256(com.google.protobuf.ByteString.copyFrom(new byte[32])))
                        .setOutputType(org.pangrova.protocol.v2.LogicalType.newBuilder().setCanonicalSql("VARCHAR")
                                .setSubstraitType(io.substrait.proto.Type.newBuilder().setString(
                                        io.substrait.proto.Type.String.newBuilder().setNullability(
                                                io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE))))
                        .setRequestLimits(org.pangrova.protocol.v2.ModelRequestLimits.newBuilder()
                                .setMaxInputTokens(1024).setMaxOutputTokens(512)
                                .setMaxRequestBytes(262144).setMaxResponseBytes(1048576)))
                .setResources(org.pangrova.protocol.v2.OperatorResourceRequirement.newBuilder().setProviderConcurrency(2)
                        .setProviderItems(1024).setProviderAttempts(4096).setProviderRequestBytes(268435456)
                        .setProviderResponseBytes(1073741824));
        return AiTestTaskAuthority.request(new AiTestTaskAuthority.Identity("tenant", "principal", "query", id,
                "trace", Instant.now().plusSeconds(30).toEpochMilli()), grant.build())
                .toBuilder().addOperatorBindings(binding).build();
    }

    /**
     * Simple stub inference provider for testing.
     */
    private static class StubInferenceProvider implements InferenceProvider {
        private String response = "";
        private int calls;

        void setResponse(String response) {
            this.response = response;
        }

        @Override
        public InferenceResult complete(String prompt, GenerationParameters parameters) {
            calls++;
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
