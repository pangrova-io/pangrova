package org.pangrova.worker.ai.inference;

import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.inference.ContentPart;
import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.inference.MultimodalPrompt;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/** Encodes exact multipart requests for /v1/audio/transcriptions. */
final class OpenAiTranscriptionRequestEncoder {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.US_ASCII);

    private final String modelName;
    private final Function<BlobReference, byte[]> blobLoader;

    OpenAiTranscriptionRequestEncoder(
            String modelName, Function<BlobReference, byte[]> blobLoader) {
        this.modelName = java.util.Objects.requireNonNull(modelName, "modelName required");
        this.blobLoader = java.util.Objects.requireNonNull(blobLoader, "blobLoader required");
    }

    EncodedRequest encode(
            MultimodalPrompt prompt, GenerationParameters parameters) {
        requireSupportedParameters(parameters);
        BlobReference reference = requireAudioReference(prompt.parts());
        byte[] audio = GovernedBlobContent.loadVerified(blobLoader, reference);
        String boundary = "pangrova-" + reference.checksum();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        appendText(body, boundary, "model", modelName);
        if (!prompt.textContent().isBlank()) {
            appendText(body, boundary, "prompt", prompt.textContent());
        }
        parameters.temperature().ifPresent(value ->
                appendText(body, boundary, "temperature", Double.toString(value)));
        appendText(body, boundary, "response_format", "json");
        appendFile(body, boundary, reference, audio);
        writeAscii(body, "--" + boundary + "--");
        body.writeBytes(CRLF);
        return new EncodedRequest(
                "multipart/form-data; boundary=" + boundary,
                body.toByteArray());
    }

    private static void requireSupportedParameters(GenerationParameters parameters) {
        if (parameters.maxTokens().isPresent() || parameters.responseFormat().isPresent()) {
            throw new AiInvocationException(
                    AiInvocationException.Category.CAPABILITY_MISMATCH,
                    false,
                    "OpenAI transcription does not accept maxTokens or responseFormat");
        }
    }

    private static BlobReference requireAudioReference(List<ContentPart> parts) {
        List<BlobReference> files = parts.stream()
                .filter(ContentPart.FileRef.class::isInstance)
                .map(ContentPart.FileRef.class::cast)
                .map(ContentPart.FileRef::reference)
                .toList();
        boolean unsupported = parts.stream().anyMatch(part ->
                !(part instanceof ContentPart.Text)
                        && !(part instanceof ContentPart.FileRef));
        if (files.size() != 1 || unsupported) {
            throw new AiInvocationException(
                    AiInvocationException.Category.INVALID_REQUEST,
                    false,
                    "OpenAI transcription requires exactly one audio FILE");
        }
        BlobReference reference = files.get(0);
        if (!reference.mimeType().toLowerCase(Locale.ROOT).startsWith("audio/")) {
            throw new AiInvocationException(
                    AiInvocationException.Category.CAPABILITY_MISMATCH,
                    false,
                    "OpenAI transcription requires audio MIME content; got "
                            + reference.mimeType());
        }
        return reference;
    }

    private static void appendText(
            ByteArrayOutputStream body,
            String boundary,
            String name,
            String value) {
        appendBoundary(body, boundary);
        writeAscii(body, "Content-Disposition: form-data; name=\"" + name + "\"");
        body.writeBytes(CRLF);
        body.writeBytes(CRLF);
        body.writeBytes(value.getBytes(StandardCharsets.UTF_8));
        body.writeBytes(CRLF);
    }

    private static void appendFile(
            ByteArrayOutputStream body,
            String boundary,
            BlobReference reference,
            byte[] audio) {
        appendBoundary(body, boundary);
        writeAscii(body,
                "Content-Disposition: form-data; name=\"file\"; filename=\"audio-"
                        + reference.checksum().substring(0, 16)
                        + extension(reference.mimeType()) + "\"");
        body.writeBytes(CRLF);
        writeAscii(body, "Content-Type: " + reference.mimeType());
        body.writeBytes(CRLF);
        body.writeBytes(CRLF);
        body.writeBytes(audio);
        body.writeBytes(CRLF);
    }

    private static void appendBoundary(ByteArrayOutputStream body, String boundary) {
        writeAscii(body, "--" + boundary);
        body.writeBytes(CRLF);
    }

    private static void writeAscii(ByteArrayOutputStream body, String value) {
        body.writeBytes(value.getBytes(StandardCharsets.US_ASCII));
    }

    private static String extension(String mimeType) {
        return switch (mimeType.toLowerCase(Locale.ROOT)) {
            case "audio/mpeg", "audio/mp3" -> ".mp3";
            case "audio/wav", "audio/x-wav" -> ".wav";
            case "audio/flac" -> ".flac";
            case "audio/ogg" -> ".ogg";
            case "audio/mp4", "audio/m4a" -> ".m4a";
            default -> ".audio";
        };
    }

    record EncodedRequest(String contentType, byte[] body) {
        EncodedRequest {
            body = body.clone();
        }

        @Override
        public byte[] body() {
            return body.clone();
        }
    }
}
