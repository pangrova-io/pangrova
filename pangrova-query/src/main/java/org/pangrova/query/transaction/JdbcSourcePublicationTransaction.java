package org.pangrova.query.transaction;

import org.pangrova.spi.annotation.ThreadSafe;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.SourcePublicationTransaction;
import org.pangrova.spi.coordination.WriteCommitEntry;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC source-publication transaction executor that runs on the caller's live
 * connection and shares its surrounding transaction and lock ordering.
 */
@ThreadSafe
public final class JdbcSourcePublicationTransaction implements SourcePublicationTransaction {

    @Override
    public ExecutionAnchor requireCurrent(Connection connection, ExecutionAnchor expected) throws SQLException {
        return JdbcExecutionAnchorStore.requireCurrent(connection, expected);
    }

    @Override
    public WriteCommitEntry lockIntent(Connection connection, String queryId) throws SQLException {
        return JdbcWriteCommitJournal.lockIntent(connection, queryId);
    }

    @Override
    public void commitPublication(
            Connection connection, WriteCommitEntry intent, NativeWriteFinalization publication)
            throws SQLException {
        JdbcWriteCommitJournal.commitPublication(connection, intent, publication);
    }

    @Override
    public void abortIntent(Connection connection, WriteCommitEntry intent) throws SQLException {
        JdbcWriteCommitJournal.abortIntent(connection, intent);
    }
}
