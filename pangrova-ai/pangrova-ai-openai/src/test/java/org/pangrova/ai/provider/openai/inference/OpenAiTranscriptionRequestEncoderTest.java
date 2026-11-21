package org.pangrova.ai.provider.openai.inference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.ContentPart;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.MultimodalPrompt;

/**
 * The multipart audio path had no direct coverage. These pin the request shape
 * and that the governed bytes are loaded exactly once (no redundant re-read).
 */
class OpenAiTranscriptionRequestEncoderTest {

    private static final String CHECKSUM = "a".repeat(64);
    private static final byte[] AUDIO = {1, 2, 3, 4};

    private static BlobReference audioReference() {
        return new BlobReference(
                "pangrova-blob://v2/" + "b".repeat(64) + "/" + "c".repeat(64) + "/" + CHECKSUM,
                "audio/wav",
                AUDIO.length,
                CHECKSUM);
    }

    private static String bodyOf(OpenAiTranscriptionRequestEncoder.EncodedRequest request) {
        return new String(request.body(), StandardCharsets.ISO_8859_1);
    }

    @Test
    void encodesModelPromptAndFileAsMultipart() {
        var loads = new AtomicInteger();
        var encoder = new OpenAiTranscriptionRequestEncoder("whisper-1", reference -> {
            loads.incrementAndGet();
            return AUDIO.clone();
        });

        OpenAiTranscriptionRequestEncoder.EncodedRequest request = encoder.encode(
                MultimodalPrompt.of("transcribe this", new ContentPart.FileRef(audioReference())),
                GenerationParameters.deterministic());

        assertThat(request.contentType())
                .startsWith("multipart/form-data; boundary=")
                .contains("pangrova-" + CHECKSUM);
        String body = bodyOf(request);
        assertThat(body).contains("name=\"model\"").contains("whisper-1");
        assertThat(body).contains("name=\"prompt\"").contains("transcribe this");
        assertThat(body).contains("name=\"response_format\"").contains("json");
        assertThat(body).contains("audio/wav");
        assertThat(body.trim()).endsWith("--pangrova-" + CHECKSUM + "--");
        // Governed bytes are read once; a second pass would duplicate a full SHA-256.
        assertThat(loads).hasValue(1);
    }

    @Test
    void omitsPromptPartWhenThePromptHasNoText() {
        var encoder = new OpenAiTranscriptionRequestEncoder(
                "whisper-1", reference -> AUDIO.clone());

        OpenAiTranscriptionRequestEncoder.EncodedRequest request = encoder.encode(
                new MultimodalPrompt(java.util.List.of(new ContentPart.FileRef(audioReference()))),
                GenerationParameters.deterministic());

        assertThat(bodyOf(request)).doesNotContain("name=\"prompt\"");
    }

    @Test
    void rejectsAPromptWithoutAudioReference() {
        var encoder = new OpenAiTranscriptionRequestEncoder(
                "whisper-1", reference -> AUDIO.clone());

        assertThatThrownBy(() -> encoder.encode(
                MultimodalPrompt.ofText("no audio here"), GenerationParameters.deterministic()))
                .isInstanceOf(AiInvocationException.class);
    }

    @Test
    void requiresANonNullLoader() {
        assertThatThrownBy(() -> new OpenAiTranscriptionRequestEncoder("whisper-1", null))
                .isInstanceOf(NullPointerException.class);
    }
}