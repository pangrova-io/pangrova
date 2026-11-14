package org.pangrova.ai.worker.function.file;

import org.pangrova.ai.worker.exception.MimeTypeNotAllowedException;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.worker.governance.MimeTypeValidator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileValidationContextTest {
    @Test
    void callerMutationCannotRelaxCapturedMimePolicy() {
        var properties = new HashMap<>(Map.of(MimeTypeValidator.ALLOWED_TYPES_PROPERTY, "image/png"));
        var validator = new MimeTypeValidator();
        var context = new FileValidationContext(properties, "catalog.schema.files", validator);
        properties.put(MimeTypeValidator.ALLOWED_TYPES_PROPERTY, "*/*");
        assertThatThrownBy(() -> context.encode(reference("application/pdf"), LocalDateTime.now()))
                .isInstanceOf(MimeTypeNotAllowedException.class);
        assertThat(FileStruct.decodeReference(context.encode(reference("image/png"), LocalDateTime.now())))
                .isEqualTo(reference("image/png"));
        assertThat(context.validator()).isSameAs(validator);
        assertThatThrownBy(() -> context.tableProperties().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void absentPolicyPreservesUnrestrictedSemantics() {
        var context = new FileValidationContext(null, "catalog.schema.files", new MimeTypeValidator());
        assertThat(context.tableProperties()).isNull();
        context.validator().validate("application/pdf", context.tableProperties(), context.tableName());
    }

    private static BlobReference reference(String mimeType) {
        String checksum = "a".repeat(64);
        return new BlobReference("pangrova-blob://v2/" + "b".repeat(64) + "/" + "c".repeat(64) + "/" + checksum,
                mimeType, 32, checksum);
    }
}
