package org.pangrova.spi.ai.inference;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;
import java.util.Objects;

/**
 * A multimodal prompt consisting of mixed content parts (text, images, files).
 * Used with {@link InferenceProvider#completeMultimodal(MultimodalPrompt, GenerationParameters)}.
 *
 * @param parts the ordered list of content parts forming the prompt
 */
@Experimental
public record MultimodalPrompt(List<ContentPart> parts) {

    public MultimodalPrompt {
        Objects.requireNonNull(parts, "parts must not be null");
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("prompt must have at least one part");
        }
        parts = List.copyOf(parts);
    }

    /**
     * Creates a multimodal prompt from a text string and a file reference.
     * This is the most common pattern: "describe this image" + image reference.
     *
     * @param text         the text instruction
     * @param fileRef      the file content part
     * @return a multimodal prompt
     */
    public static MultimodalPrompt of(String text, ContentPart.FileRef fileRef) {
        return new MultimodalPrompt(List.of(new ContentPart.Text(text), fileRef));
    }

    /**
     * Creates a text-only multimodal prompt (for uniform API usage).
     *
     * @param text the text content
     * @return a multimodal prompt containing only text
     */
    public static MultimodalPrompt ofText(String text) {
        return new MultimodalPrompt(List.of(new ContentPart.Text(text)));
    }

    /**
     * Extracts the text content from this prompt by concatenating all Text parts.
     * Used as fallback when the provider does not support multimodal input.
     *
     * @return concatenated text content
     */
    public String textContent() {
        StringBuilder sb = new StringBuilder();
        for (ContentPart part : parts) {
            if (part instanceof ContentPart.Text t) {
                sb.append(t.text());
            }
        }
        return sb.toString();
    }

    /**
     * Returns true if this prompt contains any non-text content parts.
     */
    public boolean isMultimodal() {
        return parts.stream().anyMatch(p -> !(p instanceof ContentPart.Text));
    }
}
