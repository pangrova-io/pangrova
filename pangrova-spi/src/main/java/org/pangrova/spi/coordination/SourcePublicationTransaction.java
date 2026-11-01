package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Stable;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * In-transaction access to durable write-commit and execution-anchor state for
 * source catalogs that must commit their publication on the same database
 * transaction as their own catalog head.
 *
 * <p>Implementations execute only on the caller's live {@link Connection}; the
 * caller owns the surrounding transaction and lock ordering. A catalog receives
 * its instance through wiring and never constructs a JDBC implementation
 * directly.</p>
 */
@Stable(since = "1.0")
public interface SourcePublicationTransaction {

    /** Holds the exact live owner on the caller's publication transaction. */
    ExecutionAnchor requireCurrent(Connection connection, ExecutionAnchor expected) throws SQLException;

    /** Locks and decodes the real redo intent on the caller's publication transaction. */
    WriteCommitEntry lockIntent(Connection connection, String queryId) throws SQLException;

    /**
     * Accepts the exact immutable source publication in the same transaction as
     * the actual catalog head.
     */
    void commitPublication(
            Connection connection, WriteCommitEntry intent, NativeWriteFinalization publication)
            throws SQLException;

    /** Retains an exact unpublished abort barrier on the caller's already fenced transaction. */
    void abortIntent(Connection connection, WriteCommitEntry intent) throws SQLException;
}
