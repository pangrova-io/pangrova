package org.pangrova.ai.worker.provider.governance;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.enums.EmbeddingPurpose;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.BoundAiInvocation;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Builds version-exact governed requests from the active native task context. */
final class AiInvocationRequests {

    private static final int MOST_SIGNIFICANT_BYTE_SHIFT = 24;

    private AiInvocationRequests() {}

    static AiInvocationRequest create(
            String modelName,
            String modelVersion,
            byte[] modelSignatureSha256,
            AiCapabilityDescriptor capability,
            AiExecutionMode mode,
            List<AiInvocationRequest.Input> inputs,
            GenerationParameters parameters) {
        return create(new RequestDefinition(
                modelName, modelVersion, modelSignatureSha256,
                capability, mode, inputs, parameters, Optional.empty()));
    }

    static AiInvocationRequest createEmbedding(
            String modelName,
            String modelVersion,
            byte[] modelSignatureSha256,
            AiCapabilityDescriptor capability,
            AiExecutionMode mode,
            List<AiInvocationRequest.Input> inputs,
            EmbeddingPurpose purpose) {
        return create(new RequestDefinition(
                modelName, modelVersion, modelSignatureSha256, capability,
                mode, inputs, GenerationParameters.DEFAULTS, Optional.of(purpose)));
    }

    private static AiInvocationRequest create(RequestDefinition definition) {
        requireModelSignature(definition.modelSignatureSha256());
        AiExecutionContext context = requireContext();
        BoundAiInvocation bound = requireBoundInvocation();
        bound.requireExactProvider(
                definition.modelName(), definition.modelVersion(), definition.modelSignatureSha256(),
                definition.capability().operation());
        String fingerprint = fingerprint(context, definition);
        String idempotencyBase = AiExecutionContext.providerIdempotencyBase()
                .orElseThrow(() -> new AiInvocationException(
                        AiFailureCategory.AUTHORIZATION,
                        false,
                        "AI expression has no stable operator idempotency identity"));
        return buildRequest(
                context, definition, fingerprint, idempotencyBase);
    }

    private static AiExecutionContext requireContext() {
        AiExecutionContext context = AiExecutionContext.current().orElseThrow(() ->
                new AiInvocationException(
                        AiFailureCategory.AUTHORIZATION,
                        false,
                        "AI invocation has no trusted native task context"));
        try {
            context.requireGovernedIdentity();
        } catch (IllegalStateException failure) {
            throw new AiInvocationException(
                    AiFailureCategory.AUTHORIZATION,
                    false,
                    failure.getMessage(),
                    failure);
        }
        return context;
    }

    private static BoundAiInvocation requireBoundInvocation() {
        return AiExecutionContext.boundInvocation()
                .orElseThrow(() -> new AiInvocationException(
                        AiFailureCategory.AUTHORIZATION,
                        false,
                        "AI expression has no exact Native OperatorBinding"));
    }

    private static AiInvocationRequest buildRequest(
            AiExecutionContext context,
            RequestDefinition definition,
            String fingerprint,
            String idempotencyBase) {
        String modelName = definition.modelName();
        byte[] modelSignatureSha256 = definition.modelSignatureSha256();
        AiCapabilityDescriptor capability = definition.capability();
        String requestId = context.traceId() + ":" + context.taskId() + ":"
                + fingerprint.substring(0, 16);
        String idempotencyKey = idempotencyBase + ":" + fingerprint;
        return new AiInvocationRequest(
                requestId,
                context.tenantId(),
                context.principal(),
                context.queryId(),
                context.traceId(),
                modelName,
                definition.modelVersion(),
                modelSignatureSha256,
                capability.version(),
                capability.operation(),
                definition.mode(),
                definition.embeddingPurpose(),
                definition.inputs(),
                definition.parameters(),
                context.deadline().orElseThrow(),
                idempotencyKey,
                Map.of(
                        "task_id", context.taskId(),
                        "task_attempt_base64", java.util.Base64.getEncoder()
                                .encodeToString(context.taskAttempt().toByteArray()),
                        "capability_id", capability.capabilityId(),
                        "model_signature_base64",
                        java.util.Base64.getEncoder().encodeToString(
                                modelSignatureSha256)));
    }

    private static void requireModelSignature(byte[] signature) {
        if (signature == null || signature.length != 32) {
            throw new AiInvocationException(
                    AiFailureCategory.CAPABILITY_MISMATCH,
                    false,
                    "AI model does not expose an exact 32-byte public signature");
        }
    }

    static AiCapabilityDescriptor requireCapability(
            java.util.Set<AiCapabilityDescriptor> capabilities,
            AiOperation operation) {
        return capabilities.stream()
                .filter(capability -> capability.operation() == operation)
                .findFirst()
                .orElseThrow(() -> new AiInvocationException(
                        AiFailureCategory.CAPABILITY_MISMATCH,
                        false,
                        "Provider does not advertise operation " + operation));
    }

    private static String fingerprint(AiExecutionContext context, RequestDefinition definition) {
        MessageDigest digest = Sha256Digests.sha256();
        update(digest, "pangrova.ai.invocation.v2");
        update(digest, context.tenantId());
        update(digest, context.principal());
        update(digest, context.taskAttempt().toByteArray());
        AiCapabilityDescriptor capability = definition.capability();
        update(digest, definition.modelName());
        update(digest, definition.modelVersion());
        update(digest, definition.modelSignatureSha256());
        update(digest, capability.capabilityId());
        update(digest, capability.version());
        update(digest, capability.operation().name());
        update(digest, definition.mode().name());
        update(digest, definition.embeddingPurpose().map(Enum::name).orElse(""));
        updateParameters(digest, definition.parameters());
        for (AiInvocationRequest.Input input : definition.inputs()) {
            updateInput(digest, input);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        update(digest, bytes);
    }

    private static void update(MessageDigest digest, byte[] bytes) {
        digest.update((byte) (bytes.length >>> MOST_SIGNIFICANT_BYTE_SHIFT));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }

    private static void updateParameters(
            MessageDigest digest, GenerationParameters parameters) {
        update(digest, parameters.temperature().isPresent()
                ? Double.toHexString(parameters.temperature().getAsDouble()) : "");
        update(digest, parameters.maxTokens().isPresent()
                ? Integer.toString(parameters.maxTokens().getAsInt()) : "");
        update(digest, parameters.responseFormat().orElse(""));
    }

    private static void updateInput(
            MessageDigest digest, AiInvocationRequest.Input input) {
        if (input instanceof AiInvocationRequest.TextInput text) {
            update(digest, "text");
            update(digest, text.text());
        } else if (input instanceof AiInvocationRequest.RerankInput rerank) {
            update(digest, "rerank");
            update(digest, rerank.query());
            update(digest, rerank.document());
        } else if (input instanceof AiInvocationRequest.BlobInput blob) {
            update(digest, "blob");
            updateBlob(digest, blob.reference());
        } else if (input instanceof AiInvocationRequest.VectorInput vector) {
            updateVector(digest, vector);
        } else if (input instanceof AiInvocationRequest.MultimodalInput multimodal) {
            updateMultimodal(digest, multimodal);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported canonical AI invocation input: " + input.getClass());
        }
    }

    private static void updateVector(
            MessageDigest digest, AiInvocationRequest.VectorInput vector) {
        update(digest, "vector");
        for (float value : vector.vector()) {
            update(digest, ByteBuffer.allocate(Float.BYTES)
                    .putInt(Float.floatToIntBits(value)).array());
        }
    }

    private static void updateMultimodal(
            MessageDigest digest, AiInvocationRequest.MultimodalInput multimodal) {
        update(digest, "multimodal");
        for (var part : multimodal.prompt().parts()) {
            updateContentPart(digest, part);
        }
    }

    private static void updateContentPart(
            MessageDigest digest,
            org.pangrova.ai.api.invocation.ContentPart part) {
        if (part instanceof org.pangrova.ai.api.invocation.ContentPart.Text text) {
            update(digest, "text-part");
            update(digest, text.text());
        } else if (part instanceof org.pangrova.ai.api.invocation.ContentPart.FileRef file) {
            update(digest, "file-part");
            updateBlob(digest, file.reference());
        } else if (part instanceof org.pangrova.ai.api.invocation.ContentPart.InlineImage image) {
            update(digest, "inline-image-part");
            update(digest, image.mimeType());
            update(digest, image.data());
        }
    }

    private static void updateBlob(
            MessageDigest digest,
            org.pangrova.ai.api.embedding.BlobReference reference) {
        update(digest, reference.uri());
        update(digest, reference.mimeType());
        update(digest, Long.toString(reference.size()));
        update(digest, reference.checksum());
    }

    private record RequestDefinition(
            String modelName,
            String modelVersion,
            byte[] modelSignatureSha256,
            AiCapabilityDescriptor capability,
            AiExecutionMode mode,
            List<AiInvocationRequest.Input> inputs,
            GenerationParameters parameters,
            Optional<EmbeddingPurpose> embeddingPurpose) {}
}
