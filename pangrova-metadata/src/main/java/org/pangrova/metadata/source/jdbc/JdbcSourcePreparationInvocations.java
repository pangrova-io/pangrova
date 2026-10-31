package org.pangrova.metadata.source.jdbc;

import org.pangrova.connector.spi.metadata.SourcePrepareInvocation;
import org.pangrova.connector.spi.metadata.SourcePublicationReservation;
import org.pangrova.query.api.coordination.ExecutionAnchor;

import java.sql.Connection;
import java.sql.SQLException;
import java.security.MessageDigest;
import java.util.UUID;

/** Durable SDK-call issuance and exact join receipts, on the already fenced catalog transaction. */
final class JdbcSourcePreparationInvocations {
    private static final int MAX_ISSUED_INVOCATIONS = 8192;

    private JdbcSourcePreparationInvocations() {}

    static SourcePrepareInvocation issue(Connection connection, SourcePublicationReservation reservation,
        ExecutionAnchor owner, int page, String after, String endpoint, byte[] requestSha256) throws SQLException {
        var invocation = new SourcePrepareInvocation(reservation.executionId(), reservation.fingerprint(),
            UUID.randomUUID().toString().replace("-", ""), page, after, endpoint, requestSha256);
        // The candidate row is locked by the caller, so retries cannot race past this resource bound.
        try (var statement = connection
            .prepareStatement("SELECT count(*) FROM managed_source_prepare_invocations WHERE fingerprint=?")) {
            statement.setString(1, reservation.fingerprint());
            try (var result = statement.executeQuery()) {
                result.next();
                if (result.getLong(1) >= MAX_ISSUED_INVOCATIONS) {
                    throw new IllegalStateException("Preparation invocation budget exhausted");
                }
            }
        }
        try (var statement = connection.prepareStatement("INSERT INTO managed_source_prepare_invocations "
            + "(invocation_id,fingerprint,execution_id,page_index,after_path,worker_endpoint,request_sha256,"
            + "issued_owner_id,issued_owner_incarnation,issued_owner_token,state,issued_at) "
            + "VALUES(?,?,?,?,?,?,?,?,?,?,'ISSUED',clock_timestamp())")) {
            statement.setString(1, invocation.invocationId());
            statement.setString(2, invocation.fingerprint());
            statement.setString(3, invocation.executionId());
            statement.setInt(4, invocation.pageIndex());
            statement.setString(5, invocation.afterPath());
            statement.setString(6, invocation.workerEndpoint());
            statement.setBytes(7, invocation.requestSha256());
            statement.setString(8, owner.ownerId());
            statement.setString(9, owner.ownerIncarnation());
            statement.setLong(10, owner.fencingToken());
            statement.executeUpdate();
        }
        return invocation;
    }

    static void acknowledge(Connection connection, SourcePublicationReservation reservation,
        SourcePrepareInvocation invocation, byte[] responseSha256) throws SQLException {
        if (!reservation.executionId().equals(invocation.executionId())
            || !reservation.fingerprint().equals(invocation.fingerprint())
            || responseSha256 == null || responseSha256.length != 32) {
            throw new IllegalArgumentException("SDK join receipt differs from its source invocation");
        }
        try (var statement = connection.prepareStatement("SELECT * FROM managed_source_prepare_invocations "
            + "WHERE invocation_id=? FOR UPDATE")) {
            statement.setString(1, invocation.invocationId());
            try (var result = statement.executeQuery()) {
                if (!result.next() || !invocation.executionId().equals(result.getString("execution_id"))
                    || !invocation.fingerprint().equals(result.getString("fingerprint"))
                    || invocation.pageIndex() != result.getInt("page_index")
                    || !invocation.afterPath().equals(result.getString("after_path"))
                    || !invocation.workerEndpoint().equals(result.getString("worker_endpoint"))
                    || !MessageDigest.isEqual(invocation.requestSha256(), result.getBytes("request_sha256"))) {
                    throw new IllegalArgumentException("SDK join receipt has no exact durable issuance");
                }
                if (result.getString("state").equals("DRAINED")) {
                    if (!MessageDigest.isEqual(responseSha256, result.getBytes("response_sha256"))) {
                        throw new IllegalArgumentException("SDK join replay conflicts with its exact response");
                    }
                    return;
                }
            }
        }
        try (var statement = connection.prepareStatement("UPDATE managed_source_prepare_invocations "
            + "SET state='DRAINED',response_sha256=?,drained_at=clock_timestamp() WHERE invocation_id=? "
            + "AND state='ISSUED'")) {
            statement.setBytes(1, responseSha256);
            statement.setString(2, invocation.invocationId());
            if (statement.executeUpdate() != 1) throw new IllegalStateException("SDK join receipt lost its issuance");
        }
    }

    static void requireJoinedPage(Connection connection, SourcePublicationReservation reservation,
        org.pangrova.connector.spi.metadata.SourceObjectPage page) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT 1 FROM managed_source_prepare_invocations "
            + "WHERE fingerprint=? AND execution_id=? AND page_index=? AND after_path=? AND "
            + "state='DRAINED' LIMIT 1")) {
            statement.setString(1, reservation.fingerprint());
            statement.setString(2, reservation.executionId());
            statement.setInt(3, page.index());
            statement.setString(4, page.afterPath());
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Source object page has no actual joined SDK invocation receipt");
                }
            }
        }
    }
}
