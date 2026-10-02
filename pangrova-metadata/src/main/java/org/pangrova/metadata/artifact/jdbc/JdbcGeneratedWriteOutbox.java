package org.pangrova.metadata.artifact.jdbc;

import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore;

import org.pangrova.metadata.artifact.GeneratedValueBuildSubmission;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DeferredAuthorizationScopeCodec;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.spi.coordination.WriteCommitEntry;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Atomic canonical generated graph creation and COMMITTED redo-outbox acknowledgement. */
final class JdbcGeneratedWriteOutbox {
    private JdbcGeneratedWriteOutbox() {}

    static void submit(Connection connection, WriteCommitEntry write, List<GeneratedValueBuildSubmission> builds)
            throws SQLException {
        AuthorizationTransactionGate.lockPublication(connection);
        if (write.state() != WriteCommitEntry.State.COMMITTED || builds.isEmpty()) {
            throw new IllegalArgumentException("Generated outbox requires a COMMITTED write and all declared builds");
        }
        if (lockCommittedWrite(connection, write)) {
            return;
        }
        for (var build : builds) {
            requireOriginalAuthority(write, build);
            ArtifactCurrentAuthority.requireCurrent(connection, build.dispatchSpec());
        }
        for (var build : builds) {
            JdbcArtifactBuildStore.createJobWithinTransaction(
                    connection, build.job(), build.dispatchSpec(), build.pages());
        }
        for (var build : builds) {
            JdbcGeneratedValueDescriptorStore.registerWithinTransaction(connection, build.descriptor());
        }
        try (var statement = connection.prepareStatement("UPDATE write_commit_journal "
                + "SET deferred_work_acknowledged=TRUE "
                + "WHERE query_id=? AND commit_fingerprint=? AND state='COMMITTED'")) {
            statement.setString(1, write.queryId());
            statement.setString(2, write.commitFingerprint());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Generated outbox lost its exact COMMITTED redo anchor");
            }
        }
    }

    private static void requireOriginalAuthority(WriteCommitEntry write, GeneratedValueBuildSubmission build) {
        var dispatch = build.dispatchSpec();
        var scope = DeferredAuthorizationScopeCodec.decode(write.authorizationReceipt().authorizationScope());
        if (!write.tenantId().equals(dispatch.tenantId()) || !write.principal().equals(dispatch.principal())
                || !scope.equals(dispatch.authorizationScope())) {
            throw new AuthorizationDeniedException("Generated build differs from its original authenticated authority");
        }
    }

    private static boolean lockCommittedWrite(Connection connection, WriteCommitEntry write) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT principal,deferred_authorization_scope,"
                + "deferred_work_acknowledged "
                + "FROM write_commit_journal WHERE query_id=? AND commit_fingerprint=? "
                + "AND state='COMMITTED' FOR UPDATE")) {
            statement.setString(1, write.queryId());
            statement.setString(2, write.commitFingerprint());
            try (var result = statement.executeQuery()) {
                if (!result.next() || !write.principal().equals(result.getString("principal"))
                        || !write.authorizationReceipt().equals(DeferredAuthorizationScopeCodec.decodeWriteReceipt(
                                result.getBytes("deferred_authorization_scope")))) {
                    throw new AuthorizationDeniedException("Generated outbox has no matching authenticated anchor");
                }
                return result.getBoolean("deferred_work_acknowledged");
            }
        }
    }
}
