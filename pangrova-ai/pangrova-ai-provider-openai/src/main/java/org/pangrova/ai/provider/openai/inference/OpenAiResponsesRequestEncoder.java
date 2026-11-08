package org.pangrova.ai.provider.openai.inference;

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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Encodes the OpenAI Responses API input_text/input_image/input_file dialect. */
final class OpenAiResponsesRequestEncoder {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, String> DOCUMENT_EXTENSIONS = Map.of(
            "application/pdf", ".pdf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".docx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            ".pptx",
            "text/plain", ".txt",
            "text/markdown", ".md");

    private final String modelName;
    private final Optional<Function<BlobReference, byte[]>> blobLoader;

    OpenAiResponsesRequestEncoder(
            String modelName,
            Optional<Function<BlobReference, byte[]>> blobLoader) {
        this.modelName = java.util.Objects.requireNonNull(modelName, "modelName required");
        this.blobLoader = java.util.Objects.requireNonNull(blobLoader, "blobLoader required");
    }

    String encodeText(String prompt, GenerationParameters parameters) {
        return encode(MultimodalPrompt.ofText(prompt), parameters);
    }

    String encode(MultimodalPrompt prompt, GenerationParameters parameters) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("model", modelName);
        ArrayNode content = root.putArray("input").addObject()
                .put("role", "user")
                .putArray("content");
        prompt.parts().forEach(part -> append(content, part));
        applyParameters(root, parameters);
        return serialize(root);
    }

    private void append(ArrayNode content, ContentPart part) {
        if (part instanceof ContentPart.Text text) {
            content.addObject().put("type", "input_text").put("text", text.text());
        } else if (part instanceof ContentPart.InlineImage image) {
            appendImage(content, image.mimeType(), image.data());
        } else if (part instanceof ContentPart.FileRef file) {
            appendFile(content, file.reference());
        }
    }

    private void appendFile(ArrayNode content, BlobReference reference) {
        String mimeType = reference.mimeType().toLowerCase(Locale.ROOT);
        String documentExtension = DOCUMENT_EXTENSIONS.get(mimeType);
        if (!mimeType.startsWith("image/") && documentExtension == null) {
            throw new AiInvocationException(
                    AiInvocationException.Category.CAPABILITY_MISMATCH,
                    false,
                    "OpenAI Responses FILE MIME type is not an admitted document or image: "
                            + mimeType);
        }
        Function<BlobReference, byte[]> loader = blobLoader.orElseThrow(() ->
                new AiInvocationException(
                        AiInvocationException.Category.AUTHORIZATION,
                        false,
                        "OpenAI Responses FILE access requires a governed blob loader"));
        byte[] value = GovernedBlobContent.loadVerified(loader, reference);
        if (mimeType.startsWith("image/")) {
            appendImage(content, mimeType, value);
            return;
        }
        content.addObject()
                .put("type", "input_file")
                .put("filename", filename(reference, documentExtension))
                .put("file_data", dataUrl(mimeType, value));
    }

    private static void appendImage(ArrayNode content, String mimeType, byte[] value) {
        if (!mimeType.toLowerCase(Locale.ROOT).startsWith("image/")) {
            throw new AiInvocationException(
                    AiInvocationException.Category.CAPABILITY_MISMATCH,
                    false,
                    "OpenAI Responses input_image requires image MIME content");
        }
        content.addObject()
                .put("type", "input_image")
                .put("image_url", dataUrl(mimeType, value));
    }

    private static String filename(BlobReference reference, String extension) {
        return "document-" + reference.checksum().substring(0, 16) + extension;
    }

    private static String dataUrl(String mimeType, byte[] value) {
        return "data:" + mimeType + ";base64,"
                + Base64.getEncoder().encodeToString(value);
    }

    private static void applyParameters(
            ObjectNode root, GenerationParameters parameters) {
        parameters.temperature().ifPresent(value -> root.put("temperature", value));
        parameters.maxTokens().ifPresent(value -> root.put("max_output_tokens", value));
        parameters.responseFormat().ifPresent(value -> root.putObject("text")
                .putObject("format").put("type", value));
    }

    private static String serialize(ObjectNode root) {
        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Failed to serialize Responses request", failure);
        }
    }
}
