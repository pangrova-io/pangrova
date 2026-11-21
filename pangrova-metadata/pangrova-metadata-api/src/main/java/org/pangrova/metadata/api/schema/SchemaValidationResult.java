package org.pangrova.metadata.api.schema;

import java.util.List;

/**
 * Result of schema evolution validation: compatibility flag with errors and warnings.
 */
public record SchemaValidationResult(boolean compatible, List<String> errors, List<String> warnings) {

    public SchemaValidationResult {
        errors = List.copyOf(errors);
        warnings = List.copyOf(warnings);
    }

    public static SchemaValidationResult success(List<String> warnings) {
        return new SchemaValidationResult(true, List.of(), warnings);
    }

    public static SchemaValidationResult failure(List<String> errors, List<String> warnings) {
        return new SchemaValidationResult(false, errors, warnings);
    }
}
