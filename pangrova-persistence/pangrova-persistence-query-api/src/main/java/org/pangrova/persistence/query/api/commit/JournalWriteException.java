package org.pangrova.persistence.query.api.commit;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Thrown when a journal write operation fails.
 * Wraps the underlying SQL or I/O exception with context.
 */
@Experimental
public final class JournalWriteException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final AdmissionOutcome admissionOutcome;

    public JournalWriteException(String message, Throwable cause, AdmissionOutcome admissionOutcome) {
        super(message, cause);
        this.admissionOutcome = java.util.Objects.requireNonNull(admissionOutcome, "admissionOutcome");
    }

    /** The actual transfer boundary; an absent replay row never proves rejection. */
    public AdmissionOutcome admissionOutcome() { return admissionOutcome; }

    /** Ownership of the attempted write, not query execution or a durable commit receipt. */
    public enum AdmissionOutcome {
        REJECTED_BEFORE_TRANSFER,
        ACCEPTED_UNESTABLISHED
    }
}
