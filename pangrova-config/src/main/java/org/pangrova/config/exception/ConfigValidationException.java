package org.pangrova.config.exception;

import java.util.List;

/**
 * Thrown at startup when configuration validation fails.
 * Carries a list of specific violations with clear error messages
 * indicating which config keys are invalid and why.
 */
public final class ConfigValidationException extends RuntimeException {

    private final List<String> violations;

    public ConfigValidationException(List<String> violations) {
        super("Configuration validation failed:\n  - " + String.join("\n  - ", violations));
        this.violations = List.copyOf(violations);
    }

    /** Specific configuration violations found during validation. */
    public List<String> violations() {
        return violations;
    }
}
