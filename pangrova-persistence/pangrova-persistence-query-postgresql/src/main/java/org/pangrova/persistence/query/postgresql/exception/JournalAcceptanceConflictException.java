package org.pangrova.persistence.query.postgresql.exception;

import java.sql.SQLException;
import org.pangrova.persistence.query.postgresql.journal.JournalEventKey;

/** A principal-scoped acceptance conflict that rejects only the candidate event. */
public final class JournalAcceptanceConflictException extends SQLException {
    private static final long serialVersionUID = 1L;
    private final JournalEventKey key;
    private boolean rollbackConfirmed;

    public JournalAcceptanceConflictException(JournalEventKey key, String message) {
        super(message, "23505");
        this.key = key;
    }

    public JournalEventKey key() {
        return key;
    }

    public boolean rollbackConfirmed() {
        return rollbackConfirmed;
    }

    public void confirmRollback() {
        rollbackConfirmed = true;
    }
}
