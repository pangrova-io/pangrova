package org.pangrova.query.transaction;

import org.pangrova.spi.coordination.CopyFileCommitBinding;
import org.pangrova.spi.coordination.CopyFilePublication;
import org.pangrova.spi.coordination.JournalWriteException;
import org.pangrova.spi.coordination.WriteCommitEntry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Immutable COPY file publication proof, kept separate from journal state. */
final class JdbcCopyFilePublicationStore {

    private static final String INSERT =
            "INSERT INTO copy_file_publications (claim_key, load_generation, claim_digest, "
                    + "execution_id, tenant_id, table_uuid, commit_fingerprint, snapshot_id) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (claim_key, load_generation) DO NOTHING";
    private static final String SELECT =
            "SELECT claim_digest, execution_id, tenant_id, table_uuid, commit_fingerprint, snapshot_id "
                    + "FROM copy_file_publications WHERE claim_key=? AND load_generation=?";

    private final DataSource dataSource;

    JdbcCopyFilePublicationStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    void record(WriteCommitEntry entry, CopyFilePublication publication) {
        Objects.requireNonNull(entry, "entry must not be null");
        Objects.requireNonNull(publication, "publication must not be null");
        if (entry.copyFile() == null || !entry.copyFile().equals(publication.binding())
                || !entry.queryId().equals(publication.executionId())
                || !entry.commitFingerprint().equals(publication.commitFingerprint())) {
            throw new IllegalArgumentException("COPY publication differs from its exact write intent");
        }
        try (Connection connection = dataSource.getConnection()) {
            Optional<CopyFilePublication> recorded = read(connection, entry);
            if (recorded.isPresent()) {
                requireSame(recorded.orElseThrow(), publication);
                return;
            }
            try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
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
            requireSame(read(connection, entry).orElseThrow(() ->
                    new IllegalStateException("COPY publication disappeared during concurrent replay")),
                    publication);
        } catch (SQLException failure) {
            Optional<CopyFilePublication> recorded = find(entry);
            if (recorded.isPresent()) {
                requireSame(recorded.orElseThrow(), publication);
                return;
            }
            throw new JournalWriteException("Cannot persist exact COPY file publication", failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    Optional<CopyFilePublication> find(WriteCommitEntry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        Objects.requireNonNull(entry.copyFile(), "COPY binding is required");
        try (Connection connection = dataSource.getConnection()) {
            return read(connection, entry);
        } catch (SQLException failure) {
            throw new JournalWriteException("Cannot inspect durable COPY file publication", failure,
                    JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED);
        }
    }

    private static Optional<CopyFilePublication> read(Connection connection, WriteCommitEntry entry)
            throws SQLException {
        CopyFileCommitBinding binding = entry.copyFile();
        try (PreparedStatement statement = connection.prepareStatement(SELECT)) {
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

    private static void requireSame(CopyFilePublication recorded, CopyFilePublication expected) {
        if (!recorded.equals(expected)) {
            throw new IllegalStateException("COPY file publication conflicts with immutable proof");
        }
    }
}
