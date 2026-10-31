package org.pangrova.query.store.jdbc.transaction;

import org.pangrova.common.api.annotation.ThreadSafe;
import org.pangrova.connector.spi.coordination.JournalWriteException;
import org.pangrova.connector.spi.coordination.WriteCommitEntry;
import org.pangrova.connector.spi.coordination.WriteCommitJournal;
import org.pangrova.common.file.CopyFileCommitBinding;
import org.pangrova.common.file.CopyFilePublication;
import org.pangrova.connector.spi.coordination.NativeWriteFinalization;
import org.pangrova.security.protocol.DeferredAuthorizationScopeCodec;
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

/**
 * JDBC-backed write commit journal for crash-recovery idempotency.
 *
 * <p>All operations are synchronous and block until persisted, ensuring that
 * a crash between intent-record and commit-execute leaves a recoverable
 * PENDING entry in the database.</p>
 *
 * <p>Schema (auto-created on first use):
 * <pre>{@code
 * CREATE TABLE IF NOT EXISTS write_commit_journal (
 *     query_id     VARCHAR(255) PRIMARY KEY,
 *     catalog_name VARCHAR(255) NOT NULL,
 *     table_name   VARCHAR(1024) NOT NULL,
 *     state        VARCHAR(32) NOT NULL,
 *     timestamp_ms BIGINT NOT NULL,
 *     fragments    TEXT,               -- Base64 worker commit fragments (replay payload)
 *     write_mode   VARCHAR(64),
 *     idempotency_key VARCHAR(512),
 *     tenant_id VARCHAR(255) NOT NULL,
 *     base_snapshot_id BIGINT NOT NULL,
 *     write_output_binding TEXT NOT NULL,
 *     commit_fingerprint VARCHAR(64) NOT NULL,
 *     owner_id VARCHAR(255),
 *     owner_incarnation VARCHAR(255),
 *     execution_fencing_token BIGINT NOT NULL DEFAULT 0
 * )
 * }</pre>
 */
@ThreadSafe
public final class JdbcWriteCommitJournal implements WriteCommitJournal {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriteCommitJournal.class);

    private static final String CREATE_TABLE =
            "CREATE TABLE IF NOT EXISTS write_commit_journal ("
                    + "query_id VARCHAR(255) PRIMARY KEY, "
                    + "catalog_name VARCHAR(255) NOT NULL, "
                    + "table_name VARCHAR(1024) NOT NULL, "
                    + "state VARCHAR(32) NOT NULL, "
                    + "timestamp_ms BIGINT NOT NULL, "
                    + "fragments TEXT, "
                    + "write_mode VARCHAR(64), "
                    + "idempotency_key VARCHAR(512) NOT NULL, "
                    + "tenant_id VARCHAR(255) NOT NULL, "
                    + "principal VARCHAR(255) NOT NULL, deferred_authorization_scope BYTEA NOT NULL, "
                    + "deferred_work_acknowledged BOOLEAN NOT NULL DEFAULT FALSE, "
                    + "base_snapshot_id BIGINT NOT NULL, "
                    + "write_output_binding TEXT NOT NULL, "
                    + "commit_fingerprint VARCHAR(64) NOT NULL UNIQUE, "
                    + "owner_id VARCHAR(255), "
                    + "owner_incarnation VARCHAR(255), "
                    + "execution_fencing_token BIGINT NOT NULL, "
                    + "published_version BIGINT, manifest_sha256 BYTEA, "
                    + "copy_claim_key CHAR(64), copy_load_generation BIGINT, "
                    + "copy_claim_digest CHAR(64), copy_table_uuid UUID, "
                    + "CONSTRAINT chk_write_commit_terminal_state CHECK ("
                    + "state IN ('PENDING','COMMITTED','ABORTED') "
                    + "AND (state <> 'ABORTED' OR (published_version IS NULL AND manifest_sha256 IS NULL))), "
                    + "CONSTRAINT chk_write_commit_publication CHECK ("
                    + "(published_version IS NULL AND manifest_sha256 IS NULL) OR "
                    + "(published_version IS NOT NULL AND published_version > 0 "
                    + "AND manifest_sha256 IS NOT NULL AND octet_length(manifest_sha256) = 32)), "
                    + "CONSTRAINT chk_write_commit_owner CHECK ((owner_id IS NULL "
                    + "AND owner_incarnation IS NULL AND execution_fencing_token = 0) OR "
                    + "(owner_id IS NOT NULL AND owner_incarnation IS NOT NULL "
                    + "AND execution_fencing_token > 0)), "
                    + "CONSTRAINT chk_write_commit_base_snapshot "
                    + "CHECK (base_snapshot_id >= -1), "
                    + "CONSTRAINT chk_write_commit_fingerprint "
                    + "CHECK (commit_fingerprint ~ '^[0-9a-f]{64}$'), "
                    + "CONSTRAINT chk_write_copy_binding CHECK ((copy_claim_key IS NULL "
                    + "AND copy_load_generation IS NULL AND copy_claim_digest IS NULL "
                    + "AND copy_table_uuid IS NULL) OR (copy_claim_key ~ '^[0-9a-f]{64}$' "
                    + "AND copy_load_generation >= 0 AND copy_claim_digest ~ '^[0-9a-f]{64}$' "
                    + "AND copy_table_uuid IS NOT NULL AND write_mode = 'INSERT')))";

    private static final String INSERT_ENTRY =
            "INSERT INTO write_commit_journal (query_id, catalog_name, table_name, state, "
                    + "timestamp_ms, fragments, write_mode, idempotency_key, owner_id, "
                    + "tenant_id, base_snapshot_id, commit_fingerprint, owner_incarnation, "
                    + "execution_fencing_token, write_output_binding, principal, deferred_authorization_scope, "
                    + "published_version, manifest_sha256, copy_claim_key, copy_load_generation, "
                    + "copy_claim_digest, copy_table_uuid) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (query_id) DO NOTHING";

    private static final String UPDATE_STATE =
            "UPDATE write_commit_journal SET state = ?, timestamp_ms = ? "
                    + "WHERE query_id = ? AND state = 'PENDING'";

    private static final String RECORD_PUBLICATION =
            "UPDATE write_commit_journal SET published_version = ?, manifest_sha256 = ? "
                    + "WHERE query_id = ? AND commit_fingerprint = ? AND "
                    + "((state = 'PENDING' AND published_version IS NULL AND manifest_sha256 IS NULL) "
                    + "OR (published_version = ? AND manifest_sha256 = ?))";

    private static final String SELECT_BY_QUERY_ID =
            "SELECT query_id, catalog_name, table_name, state, timestamp_ms, "
                    + "fragments, write_mode, idempotency_key, owner_id, "
                    + "tenant_id, base_snapshot_id, commit_fingerprint, owner_incarnation, "
                    + "execution_fencing_token, write_output_binding, principal, deferred_authorization_scope, "
                    + "published_version, manifest_sha256, copy_claim_key, copy_load_generation, "
                    + "copy_claim_digest, copy_table_uuid "
                    + "FROM write_commit_journal WHERE query_id = ?";

    private static final String SELECT_PENDING_PREFIX =
            "SELECT query_id, catalog_name, table_name, state, timestamp_ms, "
                    + "fragments, write_mode, idempotency_key, owner_id, "
                    + "tenant_id, base_snapshot_id, commit_fingerprint, owner_incarnation, "
                    + "execution_fencing_token, write_output_binding, principal, deferred_authorization_scope, "
                    + "published_version, manifest_sha256, copy_claim_key, copy_load_generation, "
                    + "copy_claim_digest, copy_table_uuid "
                    + "FROM write_commit_journal WHERE state = 'PENDING' AND query_id IN (";

    private static final String DELETE_ENTRY =
            "DELETE FROM write_commit_journal WHERE query_id = ? "
                    + "AND state='COMMITTED' AND deferred_work_acknowledged "
                    + "AND NOT EXISTS (SELECT 1 FROM index_commit_log e "
                    + "WHERE e.commit_fingerprint=write_commit_journal.commit_fingerprint AND NOT e.processed)";

    private static final String INSERT_COPY_PUBLICATION =
            "INSERT INTO copy_file_publications (claim_key, load_generation, claim_digest, "
                    + "execution_id, tenant_id, table_uuid, commit_fingerprint, snapshot_id) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (claim_key, load_generation) DO NOTHING";
    private static final String SELECT_COPY_PUBLICATION =
            "SELECT claim_digest, execution_id, tenant_id, table_uuid, commit_fingerprint, snapshot_id "
                    + "FROM copy_file_publications WHERE claim_key=? AND load_generation=?";

    private final DataSource dataSource;

    public JdbcWriteCommitJournal(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        ensureTableExists();
        LOG.info("event=write_commit_journal_started type=JDBC");
    }

    private void ensureTableExists() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CREATE_TABLE)) {
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=write_commit_journal_table_creation_failed error={}", e.getMessage(), e);
            throw new JournalWriteException("Failed to create write_commit_journal table", e,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
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

    private static void requireMatchingCopyAdmission(Connection connection, WriteCommitEntry entry)
            throws SQLException {
        CopyFileCommitBinding binding = entry.copyFile();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT claim_digest, execution_id, tenant_id, table_uuid "
                        + "FROM copy_file_claims WHERE claim_key=? AND load_generation=?")) {
            statement.setString(1, binding.fileKey());
            statement.setLong(2, binding.loadGeneration());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || !binding.claimDigest().equals(result.getString("claim_digest"))
                        || !entry.queryId().equals(result.getString("execution_id"))
                        || !entry.tenantId().equals(result.getString("tenant_id"))
                        || !binding.tableUuid().equals(result.getObject("table_uuid", UUID.class).toString())) {
                    throw new IllegalStateException("COPY write intent differs from durable file admission");
                }
            }
        }
    }

    @Override
    public void recordCopyFilePublication(WriteCommitEntry entry, CopyFilePublication publication) {
        Objects.requireNonNull(entry, "entry must not be null");
        Objects.requireNonNull(publication, "publication must not be null");
        if (entry.copyFile() == null || !entry.copyFile().equals(publication.binding())
                || !entry.queryId().equals(publication.executionId())
                || !entry.commitFingerprint().equals(publication.commitFingerprint())) {
            throw new IllegalArgumentException("COPY publication differs from its exact write intent");
        }
        try (Connection connection = dataSource.getConnection()) {
            // The journal may have been purged after an earlier successful publication.
            // A PostgreSQL BEFORE INSERT guard requires a live journal row, so replay
            // must inspect the immutable publication before attempting another insert.
            Optional<CopyFilePublication> recorded = readCopyPublication(connection, entry);
            if (recorded.isPresent()) {
                requireSamePublication(recorded.orElseThrow(), publication);
                return;
            }
            try (PreparedStatement insert = connection.prepareStatement(INSERT_COPY_PUBLICATION)) {
                int parameter = 1;
                insert.setString(parameter++, publication.binding().fileKey());
                insert.setLong(parameter++, publication.binding().loadGeneration());
                insert.setString(parameter++, publication.binding().claimDigest());
                insert.setString(parameter++, publication.executionId());
                insert.setString(parameter++, entry.tenantId());
                insert.setObject(parameter++, UUID.fromString(publication.binding().tableUuid()));
                insert.setString(parameter++, publication.commitFingerprint());
                insert.setLong(parameter, publication.snapshotId());
                if (insert.executeUpdate() == 1) {
                    return;
                }
            }
            requireSamePublication(readCopyPublication(connection, entry).orElseThrow(() ->
                    new IllegalStateException("COPY publication disappeared during concurrent replay")), publication);
        } catch (SQLException failure) {
            // A concurrent winner may publish and retire the journal between our
            // initial lookup and the guarded INSERT. Only its exact immutable
            // proof makes that SQL failure an idempotent success.
            Optional<CopyFilePublication> recorded = findCopyFilePublication(entry);
            if (recorded.isPresent()) {
                requireSamePublication(recorded.orElseThrow(), publication);
                return;
            }
            throw new JournalWriteException("Cannot persist exact COPY file publication", failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public Optional<CopyFilePublication> findCopyFilePublication(WriteCommitEntry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        Objects.requireNonNull(entry.copyFile(), "COPY binding is required");
        try (Connection connection = dataSource.getConnection()) {
            return readCopyPublication(connection, entry);
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot inspect durable COPY file publication", failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    private static Optional<CopyFilePublication> readCopyPublication(
            Connection connection, WriteCommitEntry entry) throws SQLException {
        CopyFileCommitBinding binding = entry.copyFile();
        try (PreparedStatement statement = connection.prepareStatement(SELECT_COPY_PUBLICATION)) {
            statement.setString(1, binding.fileKey());
            statement.setLong(2, binding.loadGeneration());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                if (!binding.claimDigest().equals(result.getString("claim_digest"))
                        || !entry.queryId().equals(result.getString("execution_id"))
                        || !entry.tenantId().equals(result.getString("tenant_id"))
                        || !binding.tableUuid().equals(result.getObject("table_uuid", UUID.class).toString())
                        || !entry.commitFingerprint().equals(result.getString("commit_fingerprint"))
                        || result.getLong("snapshot_id") <= 0) {
                    throw new IllegalStateException("COPY file publication conflicts with immutable intent");
                }
                return Optional.of(new CopyFilePublication(binding, entry.queryId(),
                        entry.commitFingerprint(), result.getLong("snapshot_id")));
            }
        }
    }

    private static void requireSamePublication(CopyFilePublication recorded, CopyFilePublication expected) {
        if (!recorded.equals(expected)) {
            throw new IllegalStateException("COPY file publication conflicts with immutable proof");
        }
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
        if (connection.getAutoCommit()) throw new IllegalArgumentException("Intent lock requires a transaction");
        try (var statement = connection.prepareStatement(SELECT_BY_QUERY_ID + " FOR UPDATE")) {
            statement.setString(1, Objects.requireNonNull(queryId, "queryId"));
            try (var result = statement.executeQuery()) {
                if (!result.next()) throw new IllegalStateException("Publication requires a durable write intent");
                return readEntry(result);
            }
        }
    }

    /** Accepts the exact immutable source publication in the same transaction as the actual catalog head. */
    public static void commitPublication(Connection connection, WriteCommitEntry intent,
            NativeWriteFinalization publication) throws SQLException {
        if (connection.getAutoCommit() || publication.outcome() != NativeWriteFinalization.Outcome.COMMITTED) {
            throw new IllegalArgumentException("Source journal commit requires confirmed transactional publication");
        }
        try (var statement = connection.prepareStatement(
                "UPDATE write_commit_journal SET state='COMMITTED',published_version=?,manifest_sha256=? "
                        + "WHERE query_id=? AND commit_fingerprint=? AND state IN ('PENDING','COMMITTED') "
                        + "AND ((published_version IS NULL AND manifest_sha256 IS NULL) "
                        + "OR (published_version=? AND manifest_sha256=?))")) {
            statement.setLong(1, publication.publishedVersion());
            statement.setBytes(2, publication.manifestSha256());
            statement.setString(3, intent.queryId());
            statement.setString(4, intent.commitFingerprint());
            statement.setLong(5, publication.publishedVersion());
            statement.setBytes(6, publication.manifestSha256());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Source publication conflicts with intent");
            }
        }
    }

    /** Retains an exact unpublished abort barrier on the caller's already fenced transaction. */
    public static void abortIntent(Connection connection, WriteCommitEntry intent) throws SQLException {
        if (connection.getAutoCommit()) throw new IllegalArgumentException("Source abort requires a transaction");
        try (var statement = connection.prepareStatement(
                "UPDATE write_commit_journal SET state='ABORTED' WHERE query_id=? AND commit_fingerprint=? "
                        + "AND state IN ('PENDING','ABORTED') AND published_version IS NULL AND "
                            + "manifest_sha256 IS NULL")) {
            statement.setString(1, intent.queryId());
            statement.setString(2, intent.commitFingerprint());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Published source intent cannot be aborted");
            }
        }
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

    private static WriteCommitEntry readEntry(ResultSet rs) throws SQLException {
        return new WriteCommitEntry(
                rs.getString("query_id"),
                rs.getString("catalog_name"),
                rs.getString("table_name"),
                WriteCommitEntry.State.valueOf(rs.getString("state")),
                rs.getLong("timestamp_ms"),
                decodeFragments(rs.getString("fragments")),
                rs.getString("write_mode"),
                rs.getString("idempotency_key"),
                rs.getString("tenant_id"),
                rs.getString("principal"),
                DeferredAuthorizationScopeCodec.decodeWriteReceipt(rs.getBytes("deferred_authorization_scope")),
                rs.getLong("base_snapshot_id"),
                Base64.getDecoder().decode(rs.getString("write_output_binding")),
                rs.getString("commit_fingerprint"),
                rs.getString("owner_id"),
                rs.getString("owner_incarnation"),
                rs.getLong("execution_fencing_token"),
                readPublication(rs), readCopyFile(rs));
    }

    private static CopyFileCommitBinding readCopyFile(ResultSet result) throws SQLException {
        String key = result.getString("copy_claim_key");
        if (key == null) {
            return null;
        }
        return new CopyFileCommitBinding(key, result.getLong("copy_load_generation"),
                result.getString("copy_claim_digest"),
                result.getObject("copy_table_uuid", UUID.class).toString());
    }

    private static NativeWriteFinalization readPublication(ResultSet result) throws SQLException {
        long version = result.getLong("published_version");
        return result.wasNull() ? null : new NativeWriteFinalization(
                NativeWriteFinalization.Outcome.COMMITTED, version, result.getBytes("manifest_sha256"));
    }

    private static void requireMatchingExistingIntent(
            Connection connection, WriteCommitEntry candidate) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(SELECT_BY_QUERY_ID)) {
            ps.setString(1, candidate.queryId());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()
                        || !candidate.commitFingerprint().equals(
                        rs.getString("commit_fingerprint"))
                        || WriteCommitEntry.State.ABORTED.name().equals(rs.getString("state"))) {
                    throw new JournalWriteException(
                            "Conflicting write commit intent for queryId=" + candidate.queryId(),
                            new IllegalStateException(
                                    "query id has a different fingerprint or a durable abort barrier"),
                            JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
                }
            }
        }
    }

    private static void requireCommittedEntry(Connection connection, String queryId) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT state FROM write_commit_journal WHERE query_id=?")) {
            statement.setString(1, queryId);
            try (var row = statement.executeQuery()) {
                if (!row.next() || !WriteCommitEntry.State.COMMITTED.name().equals(row.getString(1))) {
                    throw new IllegalStateException("Cannot commit a missing or aborted write intent: " + queryId);
                }
            }
        }
    }

    /** Encodes worker commit fragments as a comma-separated Base64 list for durable storage. */
    private static String encodeFragments(List<byte[]> fragments) {
        if (fragments == null || fragments.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fragments.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(Base64.getEncoder().encodeToString(fragments.get(i)));
        }
        return sb.toString();
    }

    /** Reconstructs worker commit fragments from the comma-separated Base64 storage form. */
    private static List<byte[]> decodeFragments(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return List.of();
        }
        List<byte[]> out = new ArrayList<>();
        for (String token : encoded.split(",")) {
            if (!token.isEmpty()) {
                out.add(Base64.getDecoder().decode(token));
            }
        }
        return out;
    }
}
