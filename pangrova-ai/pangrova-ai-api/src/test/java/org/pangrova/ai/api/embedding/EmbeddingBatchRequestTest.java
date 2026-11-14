package org.pangrova.ai.api.embedding;

import org.pangrova.ai.api.enums.EmbeddingPurpose;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingBatchRequestTest {
    @Test
    void preservesExactTextOrderIncludingEmptyWhitespaceAndUnicode() {
        var inputs = List.<EmbeddingBatchRequest.Input>of(
                new EmbeddingBatchRequest.TextInput(""),
                new EmbeddingBatchRequest.TextInput(" \n"),
                new EmbeddingBatchRequest.TextInput("数据库"));
        var request = new EmbeddingBatchRequest(EmbeddingPurpose.DOCUMENT, inputs);
        assertThat(request.inputs()).containsExactlyElementsOf(inputs);
        assertThat(request.purpose()).isEqualTo(EmbeddingPurpose.DOCUMENT);
    }

    @Test
    void copiesCallerListAndRejectsMutationOfPublishedInputs() {
        var first = new EmbeddingBatchRequest.TextInput("first");
        var source = new ArrayList<EmbeddingBatchRequest.Input>(List.of(first));
        var request = new EmbeddingBatchRequest(EmbeddingPurpose.QUERY, source);
        source.clear();
        assertThat(request.inputs()).containsExactly(first);
        assertThatThrownBy(() -> request.inputs().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void retainsBinaryIdentityWithoutLoadingObjectContent() {
        var reference = reference();
        var input = new EmbeddingBatchRequest.BlobInput(reference);
        var request = new EmbeddingBatchRequest(EmbeddingPurpose.RAW, List.of(input));
        assertThat(((EmbeddingBatchRequest.BlobInput) request.inputs().get(0)).reference()).isSameAs(reference);
    }

    @Test
    void rejectsEmptyAndNullInputsRatherThanInferringPurpose() {
        assertThatThrownBy(() -> new EmbeddingBatchRequest(EmbeddingPurpose.RAW, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EmbeddingBatchRequest(null,
                List.of(new EmbeddingBatchRequest.TextInput("text"))))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new EmbeddingBatchRequest(EmbeddingPurpose.RAW, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new EmbeddingBatchRequest(EmbeddingPurpose.RAW, Arrays.asList((EmbeddingBatchRequest.Input) null)))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullInputPayloads() {
        assertThatThrownBy(() -> new EmbeddingBatchRequest.TextInput(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new EmbeddingBatchRequest.BlobInput(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsMixedRepresentationsBeforeProviderDispatch() {
        assertThatThrownBy(() -> new EmbeddingBatchRequest(EmbeddingPurpose.DOCUMENT, List.of(
                new EmbeddingBatchRequest.TextInput("text"), new EmbeddingBatchRequest.BlobInput(reference()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one representation");
    }

    private static BlobReference reference() {
        String checksum = "a".repeat(64);
        return new BlobReference("pangrova-blob://v2/" + "b".repeat(64) + "/" + "c".repeat(64) + "/" + checksum,
                "image/png", 4L, checksum);
    }
}
