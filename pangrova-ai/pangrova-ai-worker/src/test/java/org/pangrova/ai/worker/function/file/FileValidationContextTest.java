package org.pangrova.ai.worker.function.file;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
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
        assertThatThrownBy(() -> context.validator().validate(
                "application/pdf", context.tableProperties(), context.tableName()))
                .isInstanceOf(MimeTypeValidator.MimeTypeNotAllowedException.class);
        validator.validate("image/png", context.tableProperties(), context.tableName());
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
}
