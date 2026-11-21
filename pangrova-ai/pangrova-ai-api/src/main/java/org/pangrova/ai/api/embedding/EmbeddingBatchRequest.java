package org.pangrova.ai.api.embedding;

import org.pangrova.ai.api.enums.EmbeddingPurpose;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Objects;

/** Ordered, homogeneous embedding input with one explicit semantic purpose. */
@Experimental
public record EmbeddingBatchRequest(EmbeddingPurpose purpose, List<Input> inputs) {

    /** Creates an ordered text request without inferring purpose from cardinality. */
    public static EmbeddingBatchRequest text(EmbeddingPurpose purpose, List<String> texts) {
        Objects.requireNonNull(texts, "texts required");
        List<Input> inputs = texts.stream().map(TextInput::new).map(Input.class::cast).toList();
        return new EmbeddingBatchRequest(purpose, inputs);
    }

    /** Creates an ordered governed-reference request without fetching the payloads. */
    public static EmbeddingBatchRequest blob(EmbeddingPurpose purpose, List<BlobReference> references) {
        Objects.requireNonNull(references, "references required");
        List<Input> inputs = references.stream().map(BlobInput::new).map(Input.class::cast).toList();
        return new EmbeddingBatchRequest(purpose, inputs);
    }

    /** One non-null input in the ordered provider request. */
    public sealed interface Input permits TextInput, BlobInput {}

    /** Text is preserved exactly, including empty strings and whitespace. */
    public record TextInput(String text) implements Input {
        public TextInput {
            Objects.requireNonNull(text, "text required");
        }
    }

    /** A governed claim-check reference, not an eagerly fetched media payload. */
    public record BlobInput(BlobReference reference) implements Input {
        public BlobInput {
            Objects.requireNonNull(reference, "reference required");
        }
    }

    /**
     * Validates the request without changing input values or order.
     *
     * @param purpose exact semantic purpose, never inferred from input count
     * @param inputs nonempty inputs of one modality representation
     * @throws NullPointerException if purpose, inputs or an input is null
     * @throws IllegalArgumentException if empty or mixing text and references
     */
    public EmbeddingBatchRequest {
        Objects.requireNonNull(purpose, "purpose required");
        inputs = List.copyOf(Objects.requireNonNull(inputs, "inputs required"));
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("embedding inputs must not be empty");
        }
        Class<?> inputType = inputs.get(0).getClass();
        if (inputs.stream().anyMatch(input -> input.getClass() != inputType)) {
            throw new IllegalArgumentException("embedding inputs must have one representation");
        }
    }
}
