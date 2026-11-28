package org.pangrova.persistence.query.postgresql.transaction;

import org.pangrova.connector.api.enums.NativeWriteCommitOutcome;
import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;
import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.CdcWriteCheckpoint;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.persistence.query.api.commit.WriteCommitReceipt;
import org.pangrova.common.file.CopyFileCommitBinding;
import org.pangrova.common.file.CopyFilePublication;
import org.pangrova.connector.api.commit.NativeWriteFinalization;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
 * <p>Flyway owns the journal schema. Redo binds the exact Worker fragments
 * and canonical output bytes directly through PostgreSQL binary columns.</p>
 */
@ThreadSafe
public final class JdbcWriteCommitJournal implements WriteCommitJournal {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriteCommitJournal.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String SELECT_ADMISSION_RECEIPT =
            "SELECT state, commit_fingerprint FROM write_commit_receipts WHERE query_id = ?";

    private static final String INSERT_ENTRY =
            "WITH existing AS MATERIALIZED (" + SELECT_ADMISSION_RECEIPT + "), inserted AS ("
                    + "INSERT INTO write_commit_journal (query_id, catalog_name, table_name, state, "
                    + "timestamp_ms, fragments, write_mode, idempotency_key, owner_id, "
                    + "tenant_id, base_snapshot_id, commit_fingerprint, owner_incarnation, "
                    + "execution_fencing_token, write_output_binding, principal, deferred_authorization_scope, "
                    + "published_version, manifest_sha256, copy_claim_key, copy_load_generation, "
                    + "copy_claim_digest, copy_table_uuid, cdc_stream_id, cdc_source_epoch, cdc_offset_ranges) "
                    + "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb "
                    + "WHERE NOT EXISTS (SELECT 1 FROM existing) "
                    + "ON CONFLICT (query_id) DO NOTHING RETURNING state, commit_fingerprint"
                    + ") SELECT state, commit_fingerprint FROM inserted UNION ALL "
                    + "SELECT state, commit_fingerprint FROM existing";

    private static final String UPDATE_STATE =
            "UPDATE write_commit_journal SET state = ?, timestamp_ms = ? "
                    + "WHERE query_id = ? AND state = 'PENDING' AND NOT EXISTS "
                    + "(SELECT 1 FROM execution_anchor WHERE execution_id=write_commit_journal.query_id)";

    // The CASE makes the wall-clock sample depend on obtaining the exact journal row lock.
    private static final String LOCK_OWNED_INTENT = """
            WITH owner AS MATERIALIZED (
                SELECT execution_id, expires_at
                FROM execution_anchor WHERE execution_id = ? AND fencing_token = ?
                FOR SHARE
            ), target AS MATERIALIZED (
                SELECT journal.query_id, journal.state, journal.publication_started
                FROM owner JOIN write_commit_journal journal ON journal.query_id = owner.execution_id
                WHERE journal.commit_fingerprint = ?
                FOR UPDATE OF journal
            ), authority AS MATERIALIZED (
                SELECT owner.execution_id
                FROM owner
                WHERE CASE WHEN (SELECT count(*) FROM target) <= 1
                    THEN owner.expires_at > clock_timestamp()
                    ELSE false END
            )
            """;
    private static final String RECORD_FENCED_OUTCOME = LOCK_OWNED_INTENT + """
            , transition AS (
                UPDATE write_commit_journal journal
                SET state = ?, timestamp_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint
                FROM target JOIN authority ON authority.execution_id = target.query_id
                WHERE journal.query_id = target.query_id AND target.state = 'PENDING'
                    AND (CAST(? AS boolean) OR journal.published_version IS NULL
                        AND journal.manifest_sha256 IS NULL)
                RETURNING journal.query_id
            )
            SELECT EXISTS (SELECT 1 FROM authority) AS current_owner,
                   EXISTS (SELECT 1 FROM transition)
                       OR EXISTS (SELECT 1 FROM target WHERE state = ?)
                       OR EXISTS (SELECT 1 FROM write_commit_receipts receipt
                           JOIN authority ON authority.execution_id = receipt.query_id
                           WHERE receipt.commit_fingerprint = ? AND receipt.state = ?) AS exact_outcome
            """;

    private static final String BEGIN_PUBLICATION = LOCK_OWNED_INTENT + """
            , transition AS (
                UPDATE write_commit_journal journal SET publication_started = true
                FROM target JOIN authority ON authority.execution_id = target.query_id
                WHERE journal.query_id = target.query_id AND target.state = 'PENDING'
                    AND NOT target.publication_started AND journal.published_version IS NULL
                    AND journal.manifest_sha256 IS NULL
                RETURNING journal.query_id
            )
            SELECT CASE
                WHEN NOT EXISTS (SELECT 1 FROM authority) THEN -1
                WHEN NOT EXISTS (SELECT 1 FROM target WHERE state = 'PENDING') THEN -2
                WHEN EXISTS (SELECT 1 FROM transition) THEN 1
                ELSE 0 END
            """;
    private static final String ABORT_UNSTARTED_PUBLICATION = LOCK_OWNED_INTENT + """
            , transition AS (
                UPDATE write_commit_journal journal SET state = 'ABORTED'
                FROM target JOIN authority ON authority.execution_id = target.query_id
                WHERE journal.query_id = target.query_id AND target.state = 'PENDING'
                    AND NOT target.publication_started AND journal.published_version IS NULL
                    AND journal.manifest_sha256 IS NULL
                RETURNING journal.query_id
            )
            SELECT CASE
                WHEN NOT EXISTS (SELECT 1 FROM authority) THEN -1
                WHEN NOT EXISTS (SELECT 1 FROM target) THEN -2
                WHEN EXISTS (SELECT 1 FROM transition)
                    OR EXISTS (SELECT 1 FROM target WHERE state = 'ABORTED' AND NOT publication_started)
                    THEN 1
                ELSE 0 END
            """;
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
                    + "copy_claim_digest, copy_table_uuid, cdc_stream_id, cdc_source_epoch, "
                    + "cdc_offset_ranges::text "
                    + "FROM write_commit_journal WHERE query_id = ?";

    private static final String SELECT_PENDING_PREFIX =
            "SELECT query_id, catalog_name, table_name, state, timestamp_ms, "
                    + "fragments, write_mode, idempotency_key, owner_id, "
                    + "tenant_id, base_snapshot_id, commit_fingerprint, owner_incarnation, "
                    + "execution_fencing_token, write_output_binding, principal, deferred_authorization_scope, "
                    + "published_version, manifest_sha256, copy_claim_key, copy_load_generation, "
                    + "copy_claim_digest, copy_table_uuid, cdc_stream_id, cdc_source_epoch, "
                    + "cdc_offset_ranges::text "
                    + "FROM write_commit_journal WHERE state = 'PENDING' AND query_id IN (";

    private static final String SELECT_CDC_CHECKPOINT = """
            SELECT query_id, commit_fingerprint, state, cdc_stream_id, cdc_source_epoch, cdc_offset_ranges::text
            FROM write_commit_journal
            WHERE tenant_id = ? AND cdc_stream_id = ? AND cdc_source_epoch = ? AND state IN ('PENDING', 'COMMITTED')
            ORDER BY (state = 'PENDING') DESC, timestamp_ms DESC, query_id DESC LIMIT 1
            """;

    /** Predicate shared by journal and execution retirement for the write row aliased as w. */
    public static final String ACKNOWLEDGED_WRITE = """
            w.state = 'COMMITTED' AND w.deferred_work_acknowledged
            AND NOT EXISTS (SELECT 1 FROM index_commit_log e
                            WHERE e.commit_fingerprint = w.commit_fingerprint AND NOT e.processed)
            """;
    public static final String RETIRABLE_WRITE = ACKNOWLEDGED_WRITE + """
              AND NOT EXISTS (
                  SELECT 1 FROM execution_anchor a WHERE a.execution_id = w.query_id
                  AND EXISTS (SELECT 1 FROM query_state_journal q WHERE q.query_id = w.query_id)
                  AND NOT EXISTS (SELECT 1 FROM query_state_journal q WHERE q.query_id = w.query_id
                                  AND q.state IN ('FINISHED', 'FAILED', 'CANCELLED'))
              )
            """;
    private static final String DELETE_ENTRY =
            "DELETE FROM write_commit_journal w WHERE w.query_id = ? AND " + RETIRABLE_WRITE;

    private static final String COPY_PUBLICATION_COLUMNS =
            "claim_digest, execution_id, tenant_id, table_uuid, commit_fingerprint, snapshot_id";
    private static final String SELECT_COPY_PUBLICATION = "SELECT " + COPY_PUBLICATION_COLUMNS
            + " FROM copy_file_publications WHERE claim_key=? AND load_generation=?";
    private static final String INSERT_COPY_PUBLICATION = "WITH existing AS MATERIALIZED (" + SELECT_COPY_PUBLICATION
            + "), inserted AS ("
            + "INSERT INTO copy_file_publications (claim_key, load_generation, " + COPY_PUBLICATION_COLUMNS + ") "
            + "SELECT ?, ?, ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM existing) "
            + "ON CONFLICT (claim_key, load_generation) DO NOTHING RETURNING " + COPY_PUBLICATION_COLUMNS
            + ") SELECT * FROM inserted UNION ALL SELECT * FROM existing";

    private final DataSource dataSource;

    public JdbcWriteCommitJournal(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        validateSchema();
        LOG.info("event=write_commit_journal_started type=JDBC");
    }

    private void validateSchema() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_BY_QUERY_ID + " AND FALSE")) {
            statement.setString(1, "");
            try (ResultSet ignored = statement.executeQuery()) {
                // Validate the canonical required columns; never create a parallel schema.
            }
        } catch (SQLException failure) {
            throw new JournalWriteException(
                    "Write journal schema is unavailable; initialize the canonical schema first",
                    failure, JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER);
        }
    }

    @Override
    public WriteCommitState recordIntent(WriteCommitEntry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        if (entry.state() != WriteCommitState.PENDING) {
            throw new IllegalArgumentException("Only PENDING write intents may be recorded");
        }
        if (!DeferredAuthorizationScopeCodec.decodeWriteReceipt(
                entry.authorizationReceipt().authorizationScope()).equals(entry.authorizationReceipt())) {
            throw new IllegalArgumentException("Write receipt declaration differs from its canonical scope");
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_ENTRY)) {
            int parameter = 1;
            ps.setString(parameter++, entry.queryId()); // exact compact identity lookup
            ps.setString(parameter++, entry.queryId()); // redo INSERT
            ps.setString(parameter++, entry.catalogName());
            ps.setString(parameter++, entry.tableName());
            ps.setString(parameter++, entry.state().name());
            ps.setLong(parameter++, entry.timestampMs());
            ps.setObject(parameter++, entry.fragments().toArray(byte[][]::new));
            ps.setString(parameter++, entry.writeMode());
            ps.setString(parameter++, entry.idempotencyKey());
            ps.setString(parameter++, entry.ownerId());
            ps.setString(parameter++, entry.tenantId());
            ps.setLong(parameter++, entry.baseSnapshotId());
            ps.setString(parameter++, entry.commitFingerprint());
            ps.setString(parameter++, entry.ownerIncarnation());
            ps.setLong(parameter++, entry.executionFencingToken());
            ps.setBytes(parameter++, entry.writeOutputBinding());
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
                ps.setNull(parameter++, java.sql.Types.OTHER);
            } else {
                ps.setString(parameter++, copyFile.fileKey());
                ps.setLong(parameter++, copyFile.loadGeneration());
                ps.setString(parameter++, copyFile.claimDigest());
                ps.setObject(parameter++, UUID.fromString(copyFile.tableUuid()));
            }
            CdcSourceIdentity cdcSource = entry.cdcSource();
            if (cdcSource == null) {
                ps.setNull(parameter++, java.sql.Types.VARCHAR);
                ps.setNull(parameter++, java.sql.Types.VARCHAR);
                ps.setNull(parameter, java.sql.Types.OTHER);
            } else {
                ps.setString(parameter++, cdcSource.streamId());
                ps.setString(parameter++, cdcSource.sourceEpoch());
                ps.setString(parameter, encodeCdcRanges(cdcSource));
            }
            WriteCommitState state = admitIntent(conn, ps, entry);
            LOG.info("event=write_commit_intent_resolved queryId={} catalog={} table={} state={}",
                    entry.queryId(), entry.catalogName(), entry.tableName(), state);
            return state;
        } catch (SQLException e) {
            LOG.error("event=write_commit_intent_failed queryId={} error={}",
                    entry.queryId(), e.getMessage(), e);
            throw new JournalWriteException(
                    "Failed to record write commit intent for queryId=" + entry.queryId(), e,
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
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
            // An existing immutable proof skips the INSERT trigger, even after redo retirement.
            try (PreparedStatement insert = connection.prepareStatement(INSERT_COPY_PUBLICATION)) {
                int parameter = 1;
                insert.setString(parameter++, publication.binding().fileKey());
                insert.setLong(parameter++, publication.binding().loadGeneration());
                insert.setString(parameter++, publication.binding().fileKey());
                insert.setLong(parameter++, publication.binding().loadGeneration());
                insert.setString(parameter++, publication.binding().claimDigest());
                insert.setString(parameter++, publication.executionId());
                insert.setString(parameter++, entry.tenantId());
                insert.setObject(parameter++, UUID.fromString(publication.binding().tableUuid()));
                insert.setString(parameter++, publication.commitFingerprint());
                insert.setLong(parameter, publication.snapshotId());
                try (ResultSet result = insert.executeQuery()) {
                    if (result.next()) {
                        requireSamePublication(copyPublicationFromRow(entry, result), publication);
                        return;
                    }
                }
            }
            // ON CONFLICT can detect a concurrent insert outside the first statement's snapshot.
            requireSamePublication(readCopyPublication(connection, entry).orElseThrow(() ->
                    new IllegalStateException("COPY publication disappeared during concurrent replay")), publication);
        } catch (SQLException failure) {
            // A concurrent winner may publish and retire the journal between our
            // initial snapshot and the guarded INSERT. Only its exact immutable
            // proof makes that SQL failure an idempotent success.
            Optional<CopyFilePublication> recorded = findCopyFilePublication(entry);
            if (recorded.isPresent()) {
                requireSamePublication(recorded.orElseThrow(), publication);
                return;
            }
            throw new JournalWriteException("Cannot persist exact COPY file publication", failure,
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
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
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
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
                return Optional.of(copyPublicationFromRow(entry, result));
            }
        }
    }

    private static CopyFilePublication copyPublicationFromRow(WriteCommitEntry entry, ResultSet result)
            throws SQLException {
        CopyFileCommitBinding binding = entry.copyFile();
        long snapshot = result.getLong("snapshot_id");
        if (!binding.claimDigest().equals(result.getString("claim_digest"))
                || !entry.queryId().equals(result.getString("execution_id"))
                || !entry.tenantId().equals(result.getString("tenant_id"))
                || !binding.tableUuid().equals(result.getObject("table_uuid", UUID.class).toString())
                || !entry.commitFingerprint().equals(result.getString("commit_fingerprint"))
                || snapshot <= 0) {
            throw new IllegalStateException("COPY file publication conflicts with immutable intent");
        }
        return new CopyFilePublication(binding, entry.queryId(), entry.commitFingerprint(), snapshot);
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
        if (publication.outcome() != NativeWriteCommitOutcome.COMMITTED) {
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
                        JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Failed to persist publication for queryId=" + queryId, failure,
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
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
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public void markCommitted(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_STATE)) {
            ps.setString(1, WriteCommitState.COMMITTED.name());
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
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public boolean recordTerminalOutcome(WriteCommitEntry intent, long token, WriteCommitState outcome) {
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(outcome, "outcome");
        if (outcome == WriteCommitState.PENDING || token <= 0) {
            throw new IllegalArgumentException("Durable finalization requires a terminal outcome and execution token");
        }
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(RECORD_FENCED_OUTCOME)) {
            statement.setString(1, intent.queryId());
            statement.setLong(2, token);
            statement.setString(3, intent.commitFingerprint());
            statement.setString(4, outcome.name());
            statement.setBoolean(5, outcome == WriteCommitState.COMMITTED);
            statement.setString(6, outcome.name());
            statement.setString(7, intent.commitFingerprint());
            statement.setString(8, outcome.name());
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Missing write outcome result: " + intent.queryId());
                }
                if (!result.getBoolean("current_owner")) {
                    return false;
                }
                if (!result.getBoolean("exact_outcome")) {
                    throw new IllegalStateException("Conflicting terminal write outcome: " + intent.queryId());
                }
                return true;
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot persist fenced write outcome: " + intent.queryId(), failure,
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public boolean tryBeginPublication(WriteCommitEntry intent, long fencingToken) {
        return applyPublicationGate(BEGIN_PUBLICATION, intent, fencingToken);
    }

    @Override
    public boolean abortBeforePublication(WriteCommitEntry intent, long fencingToken) {
        return applyPublicationGate(ABORT_UNSTARTED_PUBLICATION, intent, fencingToken);
    }

    private boolean applyPublicationGate(String sql, WriteCommitEntry intent, long token) {
        Objects.requireNonNull(intent, "intent");
        if (token <= 0) {
            throw new IllegalArgumentException("Durable external publication requires an execution token");
        }
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(sql)) {
            statement.setString(1, intent.queryId());
            statement.setLong(2, token);
            statement.setString(3, intent.commitFingerprint());
            try (var result = statement.executeQuery()) {
                if (!result.next()) throw new SQLException("Missing external publication gate result");
                return switch (result.getInt(1)) {
                    case 1 -> true;
                    case 0 -> false;
                    case -1 -> throw new IllegalStateException("Lost execution anchor for write " + intent.queryId());
                    case -2 -> throw new IllegalStateException("Write publication gate has no exact intent " + intent.queryId());
                    default -> throw new SQLException("Unknown external publication gate result");
                };
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot persist external publication gate: " + intent.queryId(), failure,
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
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
        if (connection.getAutoCommit() || publication.outcome() != NativeWriteCommitOutcome.COMMITTED) {
            throw new IllegalArgumentException("Source journal commit requires confirmed transactional publication");
        }
        try (var statement = connection.prepareStatement(
                "UPDATE write_commit_journal SET state='COMMITTED',published_version=?,manifest_sha256=? "
                        + "WHERE query_id=? AND commit_fingerprint=? AND state IN ('PENDING','COMMITTED') "
                        + "AND ((state = 'PENDING' AND published_version IS NULL AND manifest_sha256 IS NULL) "
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
        return getReceipt(queryId)
                .map(e -> e.state() == WriteCommitState.COMMITTED)
                .orElse(false);
    }

    @Override
    public Optional<WriteCommitReceipt> getReceipt(String queryId) {
        Objects.requireNonNull(queryId, "queryId");
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT * FROM write_commit_receipts WHERE query_id=? AND state IN ('COMMITTED','ABORTED')")) {
            statement.setString(1, queryId);
            try (var row = statement.executeQuery()) {
                if (!row.next()) return Optional.empty();
                return Optional.of(new WriteCommitReceipt(queryId, row.getString("tenant_id"),
                        row.getString("catalog_name"), row.getString("table_name"), row.getString("write_mode"),
                        row.getLong("base_snapshot_id"), row.getString("commit_fingerprint"),
                        WriteCommitState.valueOf(row.getString("state")), row.getLong("finalized_at_ms"),
                        row.getBoolean("copy_write"), row.getBoolean("cdc_write"), readPublication(row)));
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot resolve terminal write receipt", failure,
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
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
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
        return Optional.empty();
    }

    @Override
    public Optional<CdcWriteCheckpoint> findCdcCheckpoint(String tenantId, String streamId, String sourceEpoch) {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(SELECT_CDC_CHECKPOINT)) {
            statement.setString(1, tenantId);
            statement.setString(2, streamId);
            statement.setString(3, sourceEpoch);
            try (var result = statement.executeQuery()) {
                return result.next() ? Optional.of(new CdcWriteCheckpoint(result.getString("query_id"),
                        result.getString("commit_fingerprint"), WriteCommitState.valueOf(result.getString("state")),
                        readCdcSource(result))) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot resolve the durable CDC checkpoint", failure,
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
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
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
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
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
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
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
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
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
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
                        + "AND published_version IS NULL AND manifest_sha256 IS NULL AND NOT EXISTS "
                        + "(SELECT 1 FROM execution_anchor WHERE execution_id=write_commit_journal.query_id)")) {
            statement.setString(1, queryId);
            statement.setString(2, fingerprint);
            if (statement.executeUpdate() == 0) {
                throw new IllegalStateException("Abort does not match an exact unpublished pending or aborted intent");
            }
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot persist confirmed aborted write intent: " + queryId, failure,
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    @Override
    public void purge(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection(); var statement = conn.prepareStatement(DELETE_ENTRY)) {
            statement.setString(1, queryId);
            statement.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=write_commit_purge_failed queryId={} error={}",
                    queryId, e.getMessage(), e);
            throw new JournalWriteException(
                    "Failed to purge write commit entry for queryId=" + queryId, e,
                    JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    private static WriteCommitEntry readEntry(ResultSet rs) throws SQLException {
        return new WriteCommitEntry(
                rs.getString("query_id"),
                rs.getString("catalog_name"),
                rs.getString("table_name"),
                WriteCommitState.valueOf(rs.getString("state")),
                rs.getLong("timestamp_ms"),
                readFragments(rs),
                rs.getString("write_mode"),
                rs.getString("idempotency_key"),
                rs.getString("tenant_id"),
                rs.getString("principal"),
                DeferredAuthorizationScopeCodec.decodeWriteReceipt(rs.getBytes("deferred_authorization_scope")),
                rs.getLong("base_snapshot_id"),
                rs.getBytes("write_output_binding"),
                rs.getString("commit_fingerprint"),
                rs.getString("owner_id"),
                rs.getString("owner_incarnation"),
                rs.getLong("execution_fencing_token"),
                readPublication(rs), readCopyFile(rs), readCdcSource(rs));
    }

    private static CdcSourceIdentity readCdcSource(ResultSet result) throws SQLException {
        String streamId = result.getString("cdc_stream_id");
        if (streamId == null) {
            return null;
        }
        try {
            JsonNode root = JSON.readTree(result.getString("cdc_offset_ranges"));
            if (root == null || !root.isArray() || root.isEmpty()) {
                throw new SQLException("CDC source identity has no offset ranges");
            }
            List<CdcSourceIdentity.OffsetRange> ranges = new ArrayList<>();
            for (JsonNode node : root) {
                if (!node.isObject() || node.size() != 4
                        || !node.path("topic").isTextual()
                        || !node.path("partition").isIntegralNumber()
                        || !node.path("partition").canConvertToInt()
                        || !node.path("startOffset").isIntegralNumber()
                        || !node.path("startOffset").canConvertToLong()
                        || !node.path("endOffset").isIntegralNumber()
                        || !node.path("endOffset").canConvertToLong()) {
                    throw new SQLException("CDC source identity has an invalid offset range");
                }
                ranges.add(new CdcSourceIdentity.OffsetRange(node.path("topic").asText(),
                        node.path("partition").asInt(), node.path("startOffset").asLong(),
                        node.path("endOffset").asLong()));
            }
            return new CdcSourceIdentity(streamId, result.getString("cdc_source_epoch"), ranges);
        } catch (JsonProcessingException | IllegalArgumentException failure) {
            throw new SQLException("CDC source identity is not canonical", failure);
        }
    }

    private static String encodeCdcRanges(CdcSourceIdentity cdcSource) {
        ArrayNode ranges = JSON.createArrayNode();
        for (CdcSourceIdentity.OffsetRange range : cdcSource.ranges()) {
            ObjectNode node = ranges.addObject();
            node.put("topic", range.topic());
            node.put("partition", range.partition());
            node.put("startOffset", range.startOffset());
            node.put("endOffset", range.endOffset());
        }
        try {
            return JSON.writeValueAsString(ranges);
        } catch (JsonProcessingException impossible) {
            throw new IllegalStateException("Unexpected in-memory CDC range encoding failure", impossible);
        }
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
                NativeWriteCommitOutcome.COMMITTED, version, result.getBytes("manifest_sha256"));
    }

    private static WriteCommitState admitIntent(Connection connection, PreparedStatement admission,
            WriteCommitEntry candidate) throws SQLException {
        SQLException racedRetirement = null;
        try (ResultSet result = admission.executeQuery()) {
            if (result.next()) {
                return requireExactAdmittedState(result, candidate);
            }
        } catch (SQLException failure) {
            if (!"23505".equals(failure.getSQLState())) {
                throw failure;
            }
            // A competing insert and retirement can leave only the unique compact receipt.
            racedRetirement = failure;
        }
        // A concurrent winner may be absent from the INSERT statement's original snapshot.
        try (var statement = connection.prepareStatement(SELECT_ADMISSION_RECEIPT)) {
            statement.setString(1, candidate.queryId());
            try (var result = statement.executeQuery()) {
                if (result.next()) {
                    return requireExactAdmittedState(result, candidate);
                }
                if (racedRetirement != null) {
                    throw racedRetirement;
                }
                throw new JournalWriteException("Write admission has no durable receipt for queryId="
                        + candidate.queryId(), new IllegalStateException("Missing write admission receipt"),
                        JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
            }
        }
    }

    private static WriteCommitState requireExactAdmittedState(ResultSet row, WriteCommitEntry candidate)
            throws SQLException {
        if (candidate.commitFingerprint().equals(row.getString("commit_fingerprint"))) {
            WriteCommitState state = WriteCommitState.valueOf(row.getString("state"));
            if (state != WriteCommitState.ABORTED) {
                return state;
            }
        }
        throw new JournalWriteException("Conflicting write commit intent for queryId=" + candidate.queryId(),
                new IllegalStateException("query id has a different fingerprint or a durable abort barrier"),
                JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
    }

    private static void requireCommittedEntry(Connection connection, String queryId) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT state FROM write_commit_receipts WHERE query_id=?")) {
            statement.setString(1, queryId);
            try (var row = statement.executeQuery()) {
                if (!row.next() || !WriteCommitState.COMMITTED.name().equals(row.getString(1))) {
                    throw new IllegalStateException("Cannot commit a missing, aborted or fenced write intent: " + queryId);
                }
            }
        }
    }

    private static List<byte[]> readFragments(ResultSet row) throws SQLException {
        var fragments = row.getArray("fragments");
        try {
            return List.of((byte[][]) fragments.getArray());
        } finally {
            fragments.free();
        }
    }
}
