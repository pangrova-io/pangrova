package org.pangrova.query.transaction;

import org.pangrova.spi.annotation.ThreadSafe;
import org.pangrova.spi.coordination.JournalWriteException;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;
import org.pangrova.spi.coordination.CopyFileCommitBinding;
import org.pangrova.spi.coordination.CopyFilePublication;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.security.authz.DeferredAuthorizationScopeCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.DELETE_ENTRY;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.INSERT_ENTRY;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.RECORD_PUBLICATION;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.SELECT_BY_QUERY_ID;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.SELECT_PENDING_PREFIX;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.UPDATE_STATE;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.encodeFragments;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.readEntry;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.requireCommittedEntry;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.requireMatchingCopyAdmission;
import static org.pangrova.query.transaction.JdbcWriteCommitJournalMappings.requireMatchingExistingIntent;

/**
 * JDBC-backed write commit journal for crash-recovery idempotency.
 *
 * <p>All operations are synchronous and block until persisted, ensuring that
 * a crash between intent-record and commit-execute leaves a recoverable
 * PENDING entry in the database.</p>
 *
 * <p>Schema is owned by the query/transaction migration location; this store never
 * creates or alters tables. SQL mappings and row decoding live in
 * {@link JdbcWriteCommitJournalMappings}.</p>
 */
@ThreadSafe
public final class JdbcWriteCommitJournal implements WriteCommitJournal {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriteCommitJournal.class);

    private final DataSource dataSource;
    private final JdbcCopyFilePublicationStore copyPublications;

    public JdbcWriteCommitJournal(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.copyPublications = new JdbcCopyFilePublicationStore(dataSource);
        LOG.info("event=write_commit_journal_started type=JDBC");
    }

    @Override
    public void recordIntent(WriteCommitEntry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        if (entry.state() != WriteCommitEntry.State.PENDING) {
            throw new IllegalArgumentException("Only PENDING write intents may be recorded");
        }
        if (!DeferredAuthorizationScopeCodec.decodeWriteReceipt(
                entry.authorizationReceipt().authorizationScope()).equals(entry.authorizationReceipt())) {
            throw new IllegalArgumentException("Write receipt declaration differs from its canonical scope");
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_ENTRY)) {
            if (entry.copyFile() != null) {
                requireMatchingCopyAdmission(conn, entry);
            }
            int parameter = 1;
            ps.setString(parameter++, entry.queryId());
            ps.setString(parameter++, entry.catalogName());
            ps.setString(parameter++, entry.tableName());
            ps.setString(parameter++, entry.state().name());
            ps.setLong(parameter++, entry.timestampMs());
            ps.setString(parameter++, encodeFragments(entry.fragments()));
            ps.setString(parameter++, entry.writeMode());
            ps.setString(parameter++, entry.idempotencyKey());
            ps.setString(parameter++, entry.ownerId());
            ps.setString(parameter++, entry.tenantId());
            ps.setLong(parameter++, entry.baseSnapshotId());
            ps.setString(parameter++, entry.commitFingerprint());
            ps.setString(parameter++, entry.ownerIncarnation());
            ps.setLong(parameter++, entry.executionFencingToken());
            ps.setString(parameter++, Base64.getEncoder().encodeToString(
                    entry.writeOutputBinding()));
            ps.setString(parameter++, entry.principal());
            ps.setBytes(parameter++, entry.authorizationReceipt().authorizationScope());
            if (entry.publication() == null) {
                ps.setNull(parameter++, java.sql.Types.BIGINT);
                ps.setNull(parameter++, java.sql.Types.BINARY);
            } else {
                ps.setLong(parameter++, entry.publication().publishedVersion());
                ps.setBytes(parameter++, entry.publication().manifestSha256());
            }
            CopyFileCommitBinding copyFile = entry.copyFile();
            if (copyFile == null) {
                ps.setNull(parameter++, java.sql.Types.CHAR);
                ps.setNull(parameter++, java.sql.Types.BIGINT);
                ps.setNull(parameter++, java.sql.Types.CHAR);
                ps.setNull(parameter, java.sql.Types.OTHER);
            } else {
                ps.setString(parameter++, copyFile.fileKey());
                ps.setLong(parameter++, copyFile.loadGeneration());
                ps.setString(parameter++, copyFile.claimDigest());
                ps.setObject(parameter, UUID.fromString(copyFile.tableUuid()));
            }
            int inserted = ps.executeUpdate();
            if (inserted == 0) {
                requireMatchingExistingIntent(conn, entry);
            }
            LOG.info("event=write_commit_intent_recorded queryId={} catalog={} table={}",
                    entry.queryId(), entry.catalogName(), entry.tableName());
        } catch (SQLException e) {
            LOG.error("event=write_commit_intent_failed queryId={} error={}",
                    entry.queryId(), e.getMessage(), e);
            throw new JournalWriteException(
                    "Failed to record write commit intent for queryId=" + entry.queryId(), e,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public void recordCopyFilePublication(WriteCommitEntry entry, CopyFilePublication publication) {
        copyPublications.record(entry, publication);
    }

    @Override
    public Optional<CopyFilePublication> findCopyFilePublication(WriteCommitEntry entry) {
        return copyPublications.find(entry);
    }

    @Override
    public void recordPublication(String queryId, String fingerprint, NativeWriteFinalization publication) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(fingerprint, "fingerprint must not be null");
        Objects.requireNonNull(publication, "publication must not be null");
        if (publication.outcome() != NativeWriteFinalization.Outcome.COMMITTED) {
            throw new IllegalArgumentException("Only confirmed publication may be recorded");
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(RECORD_PUBLICATION)) {
            statement.setLong(1, publication.publishedVersion());
            statement.setBytes(2, publication.manifestSha256());
            statement.setString(3, queryId);
            statement.setString(4, fingerprint);
            statement.setLong(5, publication.publishedVersion());
            statement.setBytes(6, publication.manifestSha256());
            if (statement.executeUpdate() != 1) {
                throw new JournalWriteException("Publication conflicts with write intent for queryId=" + queryId,
                        new IllegalStateException("Missing intent, different fingerprint or publication"),
                        JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Failed to persist publication for queryId=" + queryId, failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public boolean requiresPublication(WriteCommitEntry intent) {
        Objects.requireNonNull(intent, "intent");
        try (var connection = dataSource.getConnection(); var query = connection.prepareStatement(
                "SELECT tenant_id,requesting_principal,candidate_catalog,candidate_schema,candidate_table "
                        + "FROM materialization.attempts WHERE execution_anchor=?")) {
            query.setString(1, intent.queryId());
            try (var rows = query.executeQuery()) {
                if (!rows.next()) return false;
                String candidate = rows.getString("candidate_catalog") + "."
                        + rows.getString("candidate_schema") + "." + rows.getString("candidate_table");
                if (!intent.tenantId().equals(rows.getString("tenant_id"))
                        || !intent.principal().equals(rows.getString("requesting_principal"))
                        || !(intent.catalogName() + "." + intent.tableName()).equals(candidate)) {
                    throw new IllegalStateException("Write intent differs from its durable publication consumer");
                }
                return true;
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot inspect required write publication", failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public void markCommitted(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_STATE)) {
            ps.setString(1, WriteCommitEntry.State.COMMITTED.name());
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, queryId);
            int updated = ps.executeUpdate();
            if (updated == 0) {
                requireCommittedEntry(conn, queryId);
            } else {
                LOG.info("event=write_commit_marked_committed queryId={}", queryId);
            }
        } catch (SQLException e) {
            LOG.error("event=write_commit_mark_failed queryId={} error={}",
                    queryId, e.getMessage(), e);
            throw new JournalWriteException(
                    "Failed to mark write commit as COMMITTED for queryId=" + queryId, e,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    /** Locks and decodes the real redo intent on the source catalog's publication connection. */
    public static WriteCommitEntry lockIntent(Connection connection, String queryId) throws SQLException {
        return JdbcWriteCommitJournalMappings.lockIntent(connection, queryId);
    }

    /** Accepts the exact immutable source publication in the same transaction as the actual catalog head. */
    public static void commitPublication(Connection connection, WriteCommitEntry intent,
            NativeWriteFinalization publication) throws SQLException {
        JdbcWriteCommitJournalMappings.commitPublication(connection, intent, publication);
    }

    /** Retains an exact unpublished abort barrier on the caller's already fenced transaction. */
    public static void abortIntent(Connection connection, WriteCommitEntry intent) throws SQLException {
        JdbcWriteCommitJournalMappings.abortIntent(connection, intent);
    }

    @Override
    public boolean isCommitted(String queryId) {
        return getEntry(queryId)
                .map(e -> e.state() == WriteCommitEntry.State.COMMITTED)
                .orElse(false);
    }

    @Override
    public Optional<WriteCommitEntry> getEntry(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_QUERY_ID)) {
            ps.setString(1, queryId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(readEntry(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("event=write_commit_lookup_failed queryId={} error={}",
                    queryId, e.getMessage(), e);
            throw new JournalWriteException(
                    "Failed to look up write commit entry for queryId=" + queryId, e,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
        return Optional.empty();
    }

    @Override
    public Optional<WriteCommitEntry> findByFingerprint(String fingerprint) {
        Objects.requireNonNull(fingerprint, "fingerprint");
        String sql = SELECT_BY_QUERY_ID.replace("WHERE query_id = ?", "WHERE commit_fingerprint = ?");
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(sql)) {
            statement.setString(1, fingerprint);
            try (var result = statement.executeQuery()) {
                return result.next() ? Optional.of(readEntry(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot resolve immutable write event authority", failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public boolean isDeferredWorkAcknowledged(String fingerprint) {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT deferred_work_acknowledged FROM write_commit_journal "
                        + "WHERE commit_fingerprint=? AND state='COMMITTED'")) {
            statement.setString(1, Objects.requireNonNull(fingerprint, "fingerprint"));
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Deferred status requires its exact COMMITTED write anchor");
                }
                return result.getBoolean(1);
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot read deferred work acknowledgement", failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public void acknowledgeNoDeferredWork(String fingerprint) {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE write_commit_journal SET deferred_work_acknowledged=TRUE "
                        + "WHERE commit_fingerprint=? AND state='COMMITTED' "
                        + "AND convert_from(deferred_authorization_scope,'UTF8')::jsonb "
                        + "#>> '{definition,hasGeneratedColumns}' = 'false'")) {
            statement.setString(1, Objects.requireNonNull(fingerprint, "fingerprint"));
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException(
                        "No-work acknowledgement requires an exact COMMITTED absence declaration");
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot acknowledge deferred write work", failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public List<WriteCommitEntry> replayPendingEntries(Set<String> executionIds) {
        Objects.requireNonNull(executionIds, "executionIds must not be null");
        if (executionIds.isEmpty()) {
            return List.of();
        }
        if (executionIds.size() > 256) {
            throw new IllegalArgumentException(
                    "write-commit recovery batch exceeds 256 executions");
        }
        List<WriteCommitEntry> entries = new ArrayList<>();
        String placeholders = String.join(",", java.util.Collections.nCopies(
                executionIds.size(), "?"));
        String sql = SELECT_PENDING_PREFIX + placeholders + ") ORDER BY timestamp_ms ASC";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int index = 1;
            for (String executionId : executionIds) {
                ps.setString(index++, executionId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    entries.add(readEntry(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("event=write_commit_replay_failed error={}", e.getMessage(), e);
            throw new JournalWriteException("Failed to replay pending write commit entries", e,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
        LOG.info("event=write_commit_replay count={}", entries.size());
        return List.copyOf(entries);
    }

    @Override
    public void markAborted(String queryId, String fingerprint) {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(fingerprint, "fingerprint");
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE write_commit_journal SET state='ABORTED' WHERE query_id=? AND commit_fingerprint=? "
                        + "AND state IN ('PENDING','ABORTED') "
                        + "AND published_version IS NULL AND manifest_sha256 IS NULL")) {
            statement.setString(1, queryId);
            statement.setString(2, fingerprint);
            if (statement.executeUpdate() == 0) {
                throw new IllegalStateException("Abort does not match an exact unpublished pending or aborted intent");
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot persist confirmed aborted write intent: " + queryId, failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public void purge(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_ENTRY)) {
            ps.setString(1, queryId);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=write_commit_purge_failed queryId={} error={}",
                    queryId, e.getMessage(), e);
            throw new JournalWriteException(
                    "Failed to purge write commit entry for queryId=" + queryId, e,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }
}
