package org.pangrova.persistence.security.postgresql.grant;

import org.pangrova.security.api.grant.PrivilegeMutation;
import org.pangrova.security.api.grant.PrivilegeMutationReceipt;
import org.pangrova.security.api.identity.UserIdentity;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;

/** Durable audit and replay evidence committed by the same transaction as the authority change. */
final class PrivilegeMutationLedger {
    private PrivilegeMutationLedger() {}

    static Optional<PrivilegeMutationReceipt> replay(Connection connection, UserIdentity actor,
            PrivilegeMutation mutation, byte[] encoded) throws SQLException {
        try (var query = connection.prepareStatement("SELECT * FROM security_privilege_mutations WHERE request_id=?")) {
            query.setObject(1, mutation.requestId());
            try (var rows = query.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                if (!actor.principal().equals(rows.getString("principal"))
                        || !Arrays.equals(encoded, rows.getBytes("request_body"))) {
                    throw new IllegalArgumentException("Privilege request ID already belongs to a different request");
                }
                return Optional.of(new PrivilegeMutationReceipt(mutation.requestId(),
                        rows.getLong("authorization_revision"), rows.getInt("changed_grants"),
                        rows.getTimestamp("recorded_at").toInstant(), true));
            }
        }
    }

    static PrivilegeMutationReceipt record(Connection connection, UserIdentity actor,
            PrivilegeMutation mutation, byte[] encoded, int changed) throws SQLException {
        String sql = "INSERT INTO security_privilege_mutations "
                + "(request_id, principal, request_body, request_description, authorization_revision, changed_grants) "
                + "SELECT ?, ?, ?, ?, revision, ? FROM pangrova_authorization_revision WHERE singleton=TRUE "
                + "RETURNING authorization_revision, recorded_at";
        try (var insert = connection.prepareStatement(sql)) {
            insert.setObject(1, mutation.requestId());
            insert.setString(2, actor.principal());
            insert.setBytes(3, encoded);
            insert.setString(4, mutation.toString());
            insert.setInt(5, changed);
            try (var rows = insert.executeQuery()) {
                if (!rows.next()) throw new IllegalStateException("Missing authorization revision authority");
                return new PrivilegeMutationReceipt(mutation.requestId(), rows.getLong(1), changed,
                        rows.getTimestamp(2).toInstant(), false);
            }
        }
    }
}
