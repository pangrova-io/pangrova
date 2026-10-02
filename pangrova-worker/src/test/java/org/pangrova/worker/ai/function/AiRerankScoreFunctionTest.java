package org.pangrova.worker.ai.function;

import org.junit.jupiter.api.Test;
import com.google.protobuf.ByteString;
import io.substrait.proto.Type;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.function.FunctionBindingDeclarations;
import org.pangrova.protocol.function.GovernedBindingProjection;
import org.pangrova.protocol.function.GovernedScalarValue;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.ModelRequestLimits;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.spi.ai.rerank.RerankingBatchResult;
import org.pangrova.spi.ai.rerank.RerankingProvider;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.worker.ai.context.AiExecutionContext;
import org.pangrova.worker.ai.context.AiTestTaskAuthority;
import org.pangrova.worker.ai.invocation.AiInvocationService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Grouping/authority unit tests; the deterministic provider is not model, transport or E2E evidence. */
class AiRerankScoreFunctionTest {

    private static final byte[] MODEL_SIGNATURE = new byte[32];

    @Test
    void groupsRowsByExactModelAndQueryAndPreservesInputOrder() {
        List<Call> calls = new ArrayList<>();
        RerankingProvider provider = new RerankingProvider() {
            @Override
            public RerankingBatchResult score(
                    String query, List<String> documents) {
                assertThat(AiExecutionContext.boundInvocation().orElseThrow()
                        .maxProviderItems()).isEqualTo(1_024);
                calls.add(new Call(query, List.copyOf(documents)));
                return new RerankingBatchResult(
                        documents.stream()
                                .map(document -> (double) document.length())
                                .toList(),
                        "");
            }

            @Override public String modelName() { return "reranker"; }
            @Override public String modelVersion() { return "v1"; }
            @Override public byte[] modelSignatureSha256() {
                return MODEL_SIGNATURE.clone();
            }
            @Override public int maxBatchSize() { return 2; }
        };
        var function = new AiRerankScoreFunction(name -> provider);

        var task = task();
        AiInvocationService.registerTask(task);
        var context = AiExecutionContext.fromTaskRequest(task);
        try {
            List<ColumnValue> result = AiExecutionContext.callWith(context,
                    () -> function.invokeBatch(List.of(
                            arguments(task, "q", "one"), arguments(task, "q", "four"),
                            arguments(task, "other", "five!"), arguments(task, "q", "seventh"),
                            arguments(task, "q", null))));
            assertThat(result.subList(0, 4))
                    .extracting(value -> ((ColumnValue.DoubleValue) value).value())
                    .containsExactly(3.0, 4.0, 5.0, 7.0);
            assertThat(result.get(4)).isInstanceOf(ColumnValue.NullValue.class);
            assertThat(calls).containsExactlyInAnyOrder(
                    new Call("q", List.of("one", "four")), new Call("q", List.of("seventh")),
                    new Call("other", List.of("five!")));
        } finally {
            AiInvocationService.releaseTask(context.tenantId(), context.taskAttempt());
        }
    }

    @Test
    void rejectsChangedCallbackAuthorityBeforeProviderLookup() {
        var task = task();
        AiInvocationService.registerTask(task);
        var context = AiExecutionContext.fromTaskRequest(task);
        var arguments = new ArrayList<>(arguments(task, "q", "document"));
        int ordinal = FunctionBindingDeclarations.governedLayout("AI_RERANK").ordinal("model_version");
        arguments.set(ordinal, new ColumnValue.StringValue("changed-version"));
        var function = new AiRerankScoreFunction(name -> {
            throw new AssertionError("Changed authority must fail before provider lookup");
        });
        try {
            assertThatThrownBy(() -> AiExecutionContext.callWith(context, () -> function.invoke(arguments)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("model_version");
        } finally {
            AiInvocationService.releaseTask(context.tenantId(), context.taskAttempt());
        }
    }

    @Test
    void rejectsMissingTaskAuthorityBeforeProviderLookup() {
        var function = new AiRerankScoreFunction(name -> {
            throw new AssertionError("Missing authority must fail before provider lookup");
        });
        assertThatThrownBy(() -> function.invoke(List.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("trusted AI execution context");
    }

    private static TaskRequest task() {
        String taskId = UUID.randomUUID().toString();
        var operator = OperatorBinding.newBuilder().setPlanNodeId("rerank")
                .setCapability(BuiltInCapabilityCatalog.crossEncoderRerankPlanRef())
                .setExecutionPolicy(ExecutionPolicy.newBuilder().setIdempotencyKey("query:stage:partition:rerank"))
                .setModel(ModelBinding.newBuilder().setModel(VersionedRef.newBuilder().setId("reranker")
                        .setVersion("v1").setSignatureSha256(ByteString.copyFrom(MODEL_SIGNATURE)))
                        .setOutputType(LogicalType.newBuilder().setCanonicalSql("DOUBLE").setSubstraitType(
                                Type.newBuilder().setFp64(Type.FP64.newBuilder()
                                        .setNullability(Type.Nullability.NULLABILITY_NULLABLE))))
                        .setRequestLimits(ModelRequestLimits.newBuilder().setMaxInputTokens(8192)
                                .setMaxOutputTokens(4096).setMaxRequestBytes(262144).setMaxResponseBytes(1048576)))
                .setResources(OperatorResourceRequirement.newBuilder().setProviderConcurrency(2)
                        .setProviderItems(1024).setProviderAttempts(4096)
                        .setProviderRequestBytes(268435456).setProviderResponseBytes(1073741824));
        return AiTestTaskAuthority.request(new AiTestTaskAuthority.Identity("tenant", "principal", "query",
                taskId, "trace", Instant.now().plusSeconds(30).toEpochMilli()), StageResourceGrant.newBuilder()
                .setGrantId(taskId + ":grant").setCpuSlots(1).setNativeMemoryBytes(1048576)
                .setProviderConcurrency(2).setProviderItems(1024).setProviderAttempts(4096)
                .setProviderRequestBytes(268435456).setProviderResponseBytes(1073741824).build())
                .toBuilder().addOperatorBindings(operator).build();
    }

    private static List<ColumnValue> arguments(TaskRequest task, String query, String document) {
        var layout = FunctionBindingDeclarations.governedLayout("AI_RERANK");
        var authority = GovernedBindingProjection.values(layout.id(), task.getOperatorBindings(0));
        return layout.arguments().stream().map(argument -> {
            if (argument.name().equals("query")) return text(query);
            if (argument.name().equals("document")) return text(document);
            return text(encode(authority.get(argument.name())));
        }).toList();
    }

    private static ColumnValue text(String value) {
        return value == null ? new ColumnValue.NullValue() : new ColumnValue.StringValue(value);
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

    private record Call(String query, List<String> documents) {}
}
