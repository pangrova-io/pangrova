package org.pangrova.coordinator.materialization;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;
import javax.sql.DataSource;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.port.PublicationVerification;
import org.pangrova.materialization.port.PublicationVerifier;

/**
 * Verifies that the committed candidate is the exact Native write captured for
 * the attempt anchor.
 *
 * <p>The durable capture row is written by a database trigger before the
 * coordinator can purge the write journal, so an optimistic or reconstructed
 * verdict is impossible: a missing or differing capture fails publication.
 */
public final class NativeCommitPublicationVerifier implements PublicationVerifier {

    private final DataSource dataSource;

    public NativeCommitPublicationVerifier(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void verify(PublicationVerification verification) {
        String anchor = verification.attempt().fence().executionAnchor();
        String candidate = verification.generation().physicalTable().catalog() + '.'
                + verification.generation().physicalTable().schema() + '.'
                + verification.generation().physicalTable().table();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT catalog_name, table_name, commit_fingerprint, manifest_sha256 "
                             + "FROM materialization.native_commit_capture WHERE query_id=?")) {
            statement.setString(1, anchor);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw MaterializationException.evidenceMismatch(
                            "publication has no native commit capture for anchor " + anchor);
                }
                String capturedCandidate = result.getString("catalog_name") + '.'
                        + result.getString("table_name");
                String manifest = HexFormat.of().formatHex(result.getBytes("manifest_sha256"));
                if (!candidate.equals(capturedCandidate)
                        || !verification.evidence().commitFingerprint()
                                .equals(result.getString("commit_fingerprint"))
                        || !verification.evidence().manifestHash().equals(manifest)) {
                    throw MaterializationException.evidenceMismatch(
                            "commit evidence does not match the native commit of " + capturedCandidate);
                }
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure(
                    "native commit verification failed", failure);
        }
    }
}
