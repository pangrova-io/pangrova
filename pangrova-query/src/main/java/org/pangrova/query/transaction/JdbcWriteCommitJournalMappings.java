package org.pangrova.query.transaction;

import org.pangrova.security.authz.DeferredAuthorizationScopeCodec;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.coordination.CopyFileCommitBinding;
import org.pangrova.spi.coordination.JournalWriteException;
import org.pangrova.spi.coordination.WriteCommitEntry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Shared SQL mappings, row decoding and durability guards behind
 * {@link JdbcWriteCommitJournal}. Every method is static and receives all
 * state explicitly; nothing is held here between calls.
 */
final class JdbcWriteCommitJournalMappings {

    static final String INSERT_ENTRY =
            "INSERT INTO write_commit_journal (query_id, catalog_name, table_name, state, "
                    + "timestamp_ms, fragments, write_mode, idempotency_key, owner_id, "
                    + "tenant_id, base_snapshot_id, commit_fingerprint, owner_incarnation, "
                    + "execution_fencing_token, write_output_binding, principal, deferred_authorization_scope, "
                    + "published_version, manifest_sha256, copy_claim_key, copy_load_generation, "
                    + "copy_claim_digest, copy_table_uuid) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (query_id) DO NOTHING";

    static final String UPDATE_STATE =
            "UPDATE write_commit_journal SET state = ?, timestamp_ms = ? "
                    + "WHERE query_id = ? AND state = 'PENDING'";

    static final String RECORD_PUBLICATION =
            "UPDATE write_commit_journal SET published_version = ?, manifest_sha256 = ? "
                    + "WHERE query_id = ? AND commit_fingerprint = ? AND "
                    + "((state = 'PENDING' AND published_version IS NULL AND manifest_sha256 IS NULL) "
                    + "OR (published_version = ? AND manifest_sha256 = ?))";

    static final String SELECT_BY_QUERY_ID =
            "SELECT query_id, catalog_name, table_name, state, timestamp_ms, "
                    + "fragments, write_mode, idempotency_key, owner_id, "
                    + "tenant_id, base_snapshot_id, commit_fingerprint, owner_incarnation, "
                    + "execution_fencing_token, write_output_binding, principal, deferred_authorization_scope, "
                    + "published_version, manifest_sha256, copy_claim_key, copy_load_generation, "
                    + "copy_claim_digest, copy_table_uuid "
                    + "FROM write_commit_journal WHERE query_id = ?";

    static final String SELECT_PENDING_PREFIX =
            "SELECT query_id, catalog_name, table_name, state, timestamp_ms, "
                    + "fragments, write_mode, idempotency_key, owner_id, "
                    + "tenant_id, base_snapshot_id, commit_fingerprint, owner_incarnation, "
                    + "execution_fencing_token, write_output_binding, principal, deferred_authorization_scope, "
                    + "published_version, manifest_sha256, copy_claim_key, copy_load_generation, "
                    + "copy_claim_digest, copy_table_uuid "
                    + "FROM write_commit_journal WHERE state = 'PENDING' AND query_id IN (";

    static final String DELETE_ENTRY =
            "DELETE FROM write_commit_journal WHERE query_id = ? "
                    + "AND state='COMMITTED' AND deferred_work_acknowledged "
                    + "AND NOT EXISTS (SELECT 1 FROM index_commit_log e "
                    + "WHERE e.commit_fingerprint=write_commit_journal.commit_fingerprint AND NOT e.processed)";

    private JdbcWriteCommitJournalMappings() {}

    static void requireMatchingCopyAdmission(Connection connection, WriteCommitEntry entry)
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

    static WriteCommitEntry lockIntent(Connection connection, String queryId) throws SQLException {
        if (connection.getAutoCommit()) throw new IllegalArgumentException("Intent lock requires a transaction");
        try (var statement = connection.prepareStatement(SELECT_BY_QUERY_ID + " FOR UPDATE")) {
            statement.setString(1, Objects.requireNonNull(queryId, "queryId"));
            try (var result = statement.executeQuery()) {
                if (!result.next()) throw new IllegalStateException("Publication requires a durable write intent");
                return readEntry(result);
            }
        }
    }

    static void commitPublication(Connection connection, WriteCommitEntry intent,
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

    static void abortIntent(Connection connection, WriteCommitEntry intent) throws SQLException {
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

    static WriteCommitEntry readEntry(ResultSet rs) throws SQLException {
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

    static void requireMatchingExistingIntent(
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

    static void requireCommittedEntry(Connection connection, String queryId) throws SQLException {
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
    static String encodeFragments(List<byte[]> fragments) {
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
    static List<byte[]> decodeFragments(String encoded) {
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
