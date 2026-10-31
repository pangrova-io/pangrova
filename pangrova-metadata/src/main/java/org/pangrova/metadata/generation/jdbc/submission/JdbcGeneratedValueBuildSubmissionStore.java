package org.pangrova.metadata.generation.jdbc.submission;

import org.pangrova.generation.spi.jdbc.GenerationTransactionAuthority;

import org.pangrova.metadata.generation.jdbc.JdbcGeneratedValueDescriptorStore;
import org.pangrova.metadata.generation.GeneratedValueBuildSubmissionStore;
import org.pangrova.metadata.exception.MetastoreException;

import org.pangrova.generation.spi.jdbc.GenerationJobSubmissionTransactions;
import org.pangrova.metadata.generation.GeneratedValueBuildSubmission;
import org.pangrova.security.api.AuthorizationDeniedException;
import org.pangrova.security.protocol.DeferredAuthorizationScopeCodec;
import org.pangrova.security.store.jdbc.AuthorizationTransactionGate;
import org.pangrova.connector.spi.coordination.WriteCommitEntry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/** Atomic canonical generated graph creation and COMMITTED redo-outbox acknowledgement. */
@org.pangrova.common.api.annotation.Internal
public final class JdbcGeneratedValueBuildSubmissionStore implements GeneratedValueBuildSubmissionStore {
    private final DataSource dataSource;
    private final GenerationTransactionAuthority transactionAuthority;
    private final GenerationJobSubmissionTransactions jobSubmissions;

    public JdbcGeneratedValueBuildSubmissionStore(
            DataSource dataSource, GenerationTransactionAuthority transactionAuthority,
            GenerationJobSubmissionTransactions jobSubmissions) {
        this.transactionAuthority = java.util.Objects.requireNonNull(transactionAuthority, "transactionAuthority");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.jobSubmissions = Objects.requireNonNull(jobSubmissions, "jobSubmissions");
    }

    @Override
    public void submitCommittedBuilds(WriteCommitEntry write, List<GeneratedValueBuildSubmission> builds) {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                submit(connection, write, List.copyOf(builds));
                connection.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Persist generated write outbox failed", failure);
        }
    }

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private void submit(Connection connection, WriteCommitEntry write,
            List<GeneratedValueBuildSubmission> builds) throws SQLException {
        AuthorizationTransactionGate.lockPublication(connection);
        if (write.state() != WriteCommitEntry.State.COMMITTED || builds.isEmpty()) {
            throw new IllegalArgumentException("Generated outbox requires a COMMITTED write and all declared builds");
        }
        if (lockCommittedWrite(connection, write)) {
            return;
        }
        for (var build : builds) {
            requireOriginalAuthority(write, build);
            transactionAuthority.requireCurrent(connection, build.dispatchSpec());
        }
        for (var build : builds) {
            jobSubmissions.createJob(connection, build.job(), build.dispatchSpec(), build.pages());
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
