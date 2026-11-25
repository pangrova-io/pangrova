package org.pangrova.persistence.query.postgresql.exception;

import org.pangrova.persistence.query.postgresql.journal.JournalEventKey;

import java.sql.SQLException;

/** Identifies the conflicting write. It can be rejected definitively only after rollback succeeds. */
public final class JournalPayloadConflictException extends SQLException {
    private static final long serialVersionUID = 1L;
    private final JournalEventKey key;
    private boolean rollbackConfirmed;

    public JournalPayloadConflictException(JournalEventKey key) {
        this(key, "Journal event ID has a different durable payload: " + key);
    }

    public JournalPayloadConflictException(JournalEventKey key, String message) {
        super(message, "23505");
        this.key = key;
    }

    public JournalEventKey key() { return key; }
    public boolean rollbackConfirmed() { return rollbackConfirmed; }
    public void confirmRollback() { rollbackConfirmed = true; }
}
