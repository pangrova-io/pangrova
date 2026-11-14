package org.pangrova.persistence.query.api.exception;

import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Thrown when a journal write operation fails.
 * Wraps the underlying SQL or I/O exception with context.
 */
@Experimental
public final class JournalWriteException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final JournalAdmissionOutcome admissionOutcome;

    public JournalWriteException(String message, Throwable cause, JournalAdmissionOutcome admissionOutcome) {
        super(message, cause);
        this.admissionOutcome = java.util.Objects.requireNonNull(admissionOutcome, "admissionOutcome");
    }

    /** The actual transfer boundary; an absent replay row never proves rejection. */
    public JournalAdmissionOutcome admissionOutcome() { return admissionOutcome; }

}
