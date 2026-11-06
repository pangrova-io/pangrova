package org.pangrova.ai.worker.function;

import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.function.spi.scalar.BatchableScalarFunction;
import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.function.spi.scalar.ScalarFunction;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.BoundAiInvocation;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import java.util.ArrayList;
import java.util.List;

/**
 * Internal adapter that removes Native-appended binding literals before a
 * public AI SQL function runs and scopes every provider call to that exact
 * model/operator identity. The hidden arguments are not part of the SQL API.
 */
final class GovernedAiScalarFunction implements BatchableScalarFunction {

    private static final List<String> HIDDEN_SIGNATURE =
            FunctionBindingDeclarations.governedLayout("AI_MODEL").signature();

    private final ScalarFunction delegate;

    GovernedAiScalarFunction(ScalarFunction delegate) {
        this.delegate = java.util.Objects.requireNonNull(
                delegate, "delegate must not be null");
    }

    @Override
    public FunctionMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public List<List<String>> getArgumentSignatures() {
        return delegate.getArgumentSignatures().stream().map(signature -> {
            List<String> internal = new ArrayList<>(signature);
            internal.addAll(HIDDEN_SIGNATURE);
            return List.copyOf(internal);
        }).toList();
    }

    @Override
    public ColumnValue invoke(List<ColumnValue> arguments) {
        Decoded decoded = decode(arguments);
        return AiExecutionContext.callWithBoundInvocation(
                decoded.binding(), () -> delegate.invoke(decoded.publicArguments()));
    }

    @Override
    public List<ColumnValue> invokeBatch(
            List<List<ColumnValue>> argumentsBatch) {
        if (argumentsBatch.isEmpty()) {
            return List.of();
        }
        List<Decoded> decoded = argumentsBatch.stream().map(this::decode).toList();
        BoundAiInvocation binding = decoded.get(0).binding();
        if (decoded.stream().anyMatch(value ->
                !sameBinding(value.binding(), binding))) {
            throw new IllegalArgumentException(
                    "AI callback batch mixes multiple exact OperatorBindings");
        }
        List<List<ColumnValue>> publicBatch = decoded.stream()
                .map(Decoded::publicArguments).toList();
        return AiExecutionContext.callWithBoundInvocation(binding, () -> {
            if (delegate instanceof BatchableScalarFunction batchable) {
                return batchable.invokeBatch(publicBatch);
            }
            return publicBatch.stream().map(delegate::invoke).toList();
        });
    }

    private Decoded decode(List<ColumnValue> arguments) {
        var decoded = new GovernedAiBindingArguments(arguments, delegate.getArgumentSignatures());
        List<ColumnValue> publicArguments = decoded.publicArguments();
        String capabilityId = decoded.text("capability_id");
        requireExpectedCapability(capabilityId, decoded.text("capability_version"),
                decoded.signature("capability_signature"));
        if (!(publicArguments.get(0) instanceof ColumnValue.StringValue model)
                || !decoded.text("model_id").equals(model.value())) {
            throw new IllegalArgumentException(
                    "SQL model name differs from its exact OperatorBinding");
        }
        return new Decoded(publicArguments, decoded.binding(operation(capabilityId)));
    }

    private static void requireExpectedCapability(
            String capabilityId, String version, byte[] signature) {
        org.pangrova.protocol.v2.CapabilityRef expected = expectedCapability(capabilityId);
        if (!version.equals(expected.getSemanticVersion())
                || !java.security.MessageDigest.isEqual(
                        signature, expected.getSignatureSha256().toByteArray())) {
            throw new IllegalArgumentException(
                    "AI callback capability identity differs from the built-in contract");
        }
    }

    private static org.pangrova.protocol.v2.CapabilityRef expectedCapability(
            String capabilityId) {
        return switch (capabilityId) {
            case org.pangrova.protocol.capability.BuiltInCapabilityCatalog.AI_TEXT_GENERATION_ID ->
                    org.pangrova.protocol.capability.BuiltInCapabilityCatalog
                            .aiTextGenerationPlanRef();
            case org.pangrova.protocol.capability.BuiltInCapabilityCatalog
                    .AI_MULTIMODAL_GENERATION_ID ->
                    org.pangrova.protocol.capability.BuiltInCapabilityCatalog
                            .aiMultimodalGenerationPlanRef();
            case org.pangrova.protocol.capability.BuiltInCapabilityCatalog.AI_EMBEDDING_ID ->
                    org.pangrova.protocol.capability.BuiltInCapabilityCatalog
                            .aiEmbeddingPlanRef();
            default -> throw new IllegalArgumentException(
                    "Unsupported AI invocation capability: " + capabilityId);
        };
    }

    private static AiCapabilityDescriptor.Operation operation(String capabilityId) {
        return switch (capabilityId) {
            case org.pangrova.protocol.capability.BuiltInCapabilityCatalog.AI_TEXT_GENERATION_ID ->
                    AiCapabilityDescriptor.Operation.TEXT_GENERATION;
            case org.pangrova.protocol.capability.BuiltInCapabilityCatalog.AI_MULTIMODAL_GENERATION_ID ->
                    AiCapabilityDescriptor.Operation.MULTIMODAL_GENERATION;
            case org.pangrova.protocol.capability.BuiltInCapabilityCatalog.AI_EMBEDDING_ID ->
                    AiCapabilityDescriptor.Operation.EMBEDDING;
            default -> throw new IllegalArgumentException(
                    "Unsupported AI invocation capability: " + capabilityId);
        };
    }

    private static boolean sameBinding(
            BoundAiInvocation left, BoundAiInvocation right) {
        return left.equals(right);
    }

    private record Decoded(
            List<ColumnValue> publicArguments,
            BoundAiInvocation binding) {}
}
