package org.pangrova.coordinator.materialization;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;
import javax.sql.DataSource;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.port.CommitRecovery;
import org.pangrova.materialization.port.PhysicalGenerationStore;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.spi.materialization.CommitEvidence;

/**
 * Authoritative Native commit recovery for unsettled materialization attempts.
 *
 * <p>A capture row is exact commit evidence. Without it, only an aborted write
 * journal or a proven-absent candidate table counts as uncommitted; every other
 * observation stays ambiguous so the candidate remains quarantined.
 */
public final class NativeCommitRecovery implements CommitRecovery {

    private static final String FORMAT_VERSION = "native-commit-v1";

    private final DataSource dataSource;
    private final PhysicalGenerationStore store;
    private final CatalogRegistry catalogs;

    public NativeCommitRecovery(DataSource dataSource, PhysicalGenerationStore store,
            CatalogRegistry catalogs) {
        this.dataSource = java.util.Objects.requireNonNull(dataSource, "dataSource");
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
    }

    @Override
    public Outcome recover(RefreshAttempt attempt) {
        Optional<Capture> capture = capture(attempt.fence().executionAnchor());
        if (capture.isPresent()) {
            Capture found = capture.orElseThrow();
            if (!found.tenant().equals(attempt.object().tenant().value())
                    || !found.candidate().equals(candidateName(attempt.candidateTable()))) {
                return new Unknown("native commit capture belongs to a different candidate");
            }
            return new Committed(new CommitEvidence(
                    connectorId(attempt.candidateTable().catalog()),
                    found.commitFingerprint(), found.manifestHex(), FORMAT_VERSION,
                    found.committedAt()));
        }
        Optional<String> journalState = journalState(attempt.fence().executionAnchor());
        if (journalState.isPresent()) {
            String state = journalState.orElseThrow();
            if (state.equals("ABORTED")) {
                return new Uncommitted("durable write journal recorded an abort");
            }
            return new Unknown("write journal state " + state + " has no native commit capture");
        }
        if (store.exists(attempt.object(), attempt.candidateTable())) {
            return new Unknown("candidate table exists without a durable commit record");
        }
        return new Uncommitted("no commit record and the candidate table is absent");
    }

    private Optional<Capture> capture(String anchor) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT tenant_id, catalog_name, table_name, commit_fingerprint, "
                             + "manifest_sha256, committed_at "
                             + "FROM materialization.native_commit_capture WHERE query_id=?")) {
            statement.setString(1, anchor);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(new Capture(
                        result.getString("tenant_id"),
                        result.getString("catalog_name") + '.' + result.getString("table_name"),
                        result.getString("commit_fingerprint"),
                        HexFormat.of().formatHex(result.getBytes("manifest_sha256")),
                        result.getObject("committed_at", java.time.OffsetDateTime.class)
                                .toInstant()));
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure(
                    "native commit recovery failed", failure);
        }
    }

    private Optional<String> journalState(String anchor) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT state FROM write_commit_journal WHERE query_id=?")) {
            statement.setString(1, anchor);
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                        ? Optional.of(result.getString("state")) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure(
                    "write journal lookup failed", failure);
        }
    }

    private String connectorId(String catalog) {
        return catalogs.getCatalog(catalog).getDescriptor().provider().toLowerCase(Locale.ROOT);
    }

    private static String candidateName(PhysicalTableRef table) {
        return table.catalog() + '.' + table.schema() + '.' + table.table();
    }

    private record Capture(String tenant, String candidate, String commitFingerprint,
            String manifestHex, Instant committedAt) {
    }
}
