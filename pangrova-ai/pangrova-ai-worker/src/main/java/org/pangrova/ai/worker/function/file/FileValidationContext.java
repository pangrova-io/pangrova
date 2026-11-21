package org.pangrova.ai.worker.function.file;

import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.worker.governance.MimeTypeValidator;
import org.pangrova.types.value.ColumnValue;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Snapshot of the table MIME policy for constructing a FILE row.
 *
 * <p>Groups the table-scoped inputs needed to enforce the MIME type whitelist
 * when constructing a FILE struct.</p>
 *
 * @param tableProperties table properties carrying the allowed-types setting (may be null)
 * @param tableName       qualified table name used in error messages
 * @param validator       MIME type validator instance (may be null to skip validation)
 */
public record FileValidationContext(
        Map<String, String> tableProperties,
        String tableName,
        MimeTypeValidator validator) {

    /** Captures the table policy while borrowing the thread-safe validator. */
    public FileValidationContext {
        tableProperties = tableProperties == null ? null : Map.copyOf(tableProperties);
    }

    /** Validates this table's policy before returning the encoded FILE value. */
    public ColumnValue.RowValue encode(BlobReference reference, LocalDateTime createdAt) {
        if (validator != null && tableProperties != null) {
            validator.validate(reference.mimeType(), tableProperties, tableName);
        }
        return FileStruct.encode(reference, createdAt);
    }
}
