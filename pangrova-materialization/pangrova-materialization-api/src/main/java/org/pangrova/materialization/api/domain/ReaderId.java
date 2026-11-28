package org.pangrova.materialization.api.domain;

import org.pangrova.execution.api.identity.QueryId;
import org.pangrova.materialization.api.enums.ReaderKind;

import java.util.Objects;
import java.util.UUID;

import static org.pangrova.common.validation.Preconditions.requireText;

/** Durable identity for a generation reader. An attempt and a query may have identical textual IDs. */
public record ReaderId(ReaderKind kind, String id) {
    public ReaderId {
        Objects.requireNonNull(kind, "kind");
        requireText(id, "reader id");
        if (id.length() > 128) {
            throw new IllegalArgumentException("reader id exceeds 128 characters");
        }
        if (kind == ReaderKind.REFRESH_ATTEMPT && !UUID.fromString(id).toString().equals(id)) {
            throw new IllegalArgumentException("refresh attempt reader requires a canonical UUID");
        }
    }

    public static ReaderId query(QueryId query) {
        return new ReaderId(ReaderKind.QUERY, query.id());
    }

    public static ReaderId refreshAttempt(AttemptId attempt) {
        return new ReaderId(ReaderKind.REFRESH_ATTEMPT, attempt.value().toString());
    }
}
