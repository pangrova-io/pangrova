package org.pangrova.worker.ai.function.file;

import org.pangrova.worker.ai.governance.MimeTypeValidator;

import java.util.Map;

/**
 * Validation context for {@link FileStructHelper#createFileStructValidated}.
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
}
