package org.pangrova.ai.worker.inference.openai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.invocation.ContentPart;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.MultimodalPrompt;

import java.util.Base64;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

/** Encodes the exact OpenAI chat-completions request dialect. */
final class OpenAiChatRequestEncoder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String modelName;
    private final Optional<Function<BlobReference, byte[]>> blobLoader;

    OpenAiChatRequestEncoder(
            String modelName,
            Optional<Function<BlobReference, byte[]>> blobLoader) {
        this.modelName = java.util.Objects.requireNonNull(modelName, "modelName required");
        this.blobLoader = java.util.Objects.requireNonNull(blobLoader, "blobLoader required");
    }

    boolean supportsImages() {
        return blobLoader.isPresent();
    }

    String encodeText(String prompt, GenerationParameters parameters) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("model", modelName);
        root.putArray("messages").addObject()
                .put("role", "user")
                .put("content", prompt);
        applyGenerationParameters(root, parameters);
        return serialize(root);
    }

    String encodeMultimodal(
            MultimodalPrompt prompt, GenerationParameters parameters) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("model", modelName);
        ArrayNode content = root.putArray("messages").addObject()
                .put("role", "user")
                .putArray("content");
        prompt.parts().forEach(part -> append(content, part));
        applyGenerationParameters(root, parameters);
        return serialize(root);
    }

    private void append(ArrayNode content, ContentPart part) {
        if (part instanceof ContentPart.Text text) {
            content.addObject().put("type", "text").put("text", text.text());
        } else if (part instanceof ContentPart.InlineImage image) {
            appendImage(content, image.mimeType(), image.data());
        } else if (part instanceof ContentPart.FileRef file) {
            BlobReference reference = file.reference();
            if (!reference.mimeType().toLowerCase(Locale.ROOT).startsWith("image/")) {
                throw new AiInvocationException(
                        AiInvocationException.Category.CAPABILITY_MISMATCH,
                        false,
                        "OpenAI chat-completions accepts only image FILE inputs; got "
                                + reference.mimeType());
            }
            Function<BlobReference, byte[]> loader = blobLoader.orElseThrow(() ->
                    new AiInvocationException(
                            AiInvocationException.Category.AUTHORIZATION,
                            false,
                            "Multimodal FileRef access requires a governed blob loader"));
            appendImage(
                    content,
                    reference.mimeType(),
                    GovernedBlobContent.loadVerified(loader, reference));
        }
    }

    private static void appendImage(
            ArrayNode content, String mimeType, byte[] media) {
        content.addObject()
                .put("type", "image_url")
                .putObject("image_url")
                .put("url", "data:" + mimeType + ";base64,"
                        + Base64.getEncoder().encodeToString(media));
    }

    private static void applyGenerationParameters(
            ObjectNode root, GenerationParameters parameters) {
        parameters.temperature().ifPresent(value -> root.put("temperature", value));
        parameters.maxTokens().ifPresent(value -> root.put("max_tokens", value));
        parameters.responseFormat().ifPresent(value ->
                root.putObject("response_format").put("type", value));
    }

    private static String serialize(ObjectNode root) {
        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Failed to serialize inference request", failure);
        }
    }

}
