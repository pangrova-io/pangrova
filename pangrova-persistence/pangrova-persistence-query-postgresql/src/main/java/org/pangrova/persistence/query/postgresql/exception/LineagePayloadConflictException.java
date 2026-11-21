package org.pangrova.persistence.query.postgresql.exception;

import java.sql.SQLException;

/** The immutable lineage identity already has a different payload. */
public final class LineagePayloadConflictException extends SQLException {
    private static final long serialVersionUID = 1L;

    public LineagePayloadConflictException(java.util.UUID identity) {
        super("Lineage identity has a conflicting immutable payload: " + identity, "23505");
    }
}
