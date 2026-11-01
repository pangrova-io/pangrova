package org.pangrova.persistence.jdbc.generation.job;

import org.pangrova.spi.generation.GenerationPageKind;
import org.pangrova.persistence.jdbc.generation.job.PostgresGenerationJobStoreMappings.PageRow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;

import static org.pangrova.common.jdbc.JdbcSupport.getInstant;
import static org.pangrova.common.jdbc.JdbcSupport.setInstant;

/** One page-selection contract for ordinary polling and exact durable stage partitions. */
final class PostgresGenerationPageSelection {
    private PostgresGenerationPageSelection() {}

    record Coordinates(int stageId, int partitionId, int pageOrdinal) {
        Coordinates {
            if (stageId < 0 || partitionId < 0 || pageOrdinal < 0) {
                throw new IllegalArgumentException("Durable page coordinates must be nonnegative");
            }
        }
    }

    record Selection(String tenantId, String jobId, Instant now, Optional<Coordinates> coordinates) {}

    static Optional<PageRow> selectClaimablePage(Connection connection, Selection selection)
            throws SQLException {
        String sql = "SELECT * FROM artifact_build_pages "
                + "WHERE tenant_id=? AND job_id=? "
                + "AND ((state='PENDING' AND next_available_at<=?) "
                + "OR (state='LEASED' AND lease_expires_at<?)) "
                + "AND NOT EXISTS (SELECT 1 FROM artifact_build_page_dependencies d "
                + "JOIN artifact_build_pages u ON u.tenant_id=d.tenant_id "
                + "AND u.job_id=d.job_id "
                + "AND u.page_id=d.dependency_page_id WHERE d.job_id=artifact_build_pages.job_id "
                + "AND d.tenant_id=artifact_build_pages.tenant_id "
                + "AND d.page_id=artifact_build_pages.page_id AND u.state<>'SUCCEEDED') "
                + "AND NOT EXISTS (SELECT 1 FROM artifact_stage_exchanges e WHERE "
                + "e.tenant_id=artifact_build_pages.tenant_id AND e.job_id=artifact_build_pages.job_id "
                + "AND e.consumer_stage_id=artifact_build_pages.stage_id AND NOT EXISTS "
                + "(SELECT 1 FROM artifact_exchange_finalizations f WHERE f.tenant_id=e.tenant_id "
                + "AND f.job_id=e.job_id AND f.exchange_id=e.exchange_id)) "
                + (selection.coordinates().isPresent() ? "AND stage_id=? AND partition_id=? AND page_ordinal=? " : "")
                + "ORDER BY page_ordinal LIMIT 1 FOR UPDATE SKIP LOCKED";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, selection.tenantId());
            statement.setString(2, selection.jobId());
            setInstant(statement, 3, selection.now());
            setInstant(statement, 4, selection.now());
            if (selection.coordinates().isPresent()) {
                var coordinates = selection.coordinates().orElseThrow();
                statement.setInt(5, coordinates.stageId());
                statement.setInt(6, coordinates.partitionId());
                statement.setInt(7, coordinates.pageOrdinal());
            }
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapPage(result)) : Optional.empty();
            }
        }
    }

    static void requireCoordinates(Connection connection, String tenantId, String jobId, Coordinates coordinates)
            throws SQLException {
        try (var statement = connection.prepareStatement("SELECT count(*) FROM artifact_build_pages "
                + "WHERE tenant_id=? AND job_id=? AND stage_id=? AND partition_id=? AND page_ordinal=?")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setInt(3, coordinates.stageId());
            statement.setInt(4, coordinates.partitionId());
            statement.setInt(5, coordinates.pageOrdinal());
            try (var result = statement.executeQuery()) {
                if (!result.next() || result.getLong(1) != 1) {
                    throw new IllegalStateException(
                            "Durable stage partition lacks one exact immutable page allocation");
                }
            }
        }
    }

    static Optional<PageRow> findPage(
            Connection connection, String tenantId, String jobId,
            String pageId, boolean lock) throws SQLException {
        String sql = "SELECT * FROM artifact_build_pages "
                + "WHERE tenant_id=? AND job_id=? AND page_id=?"
                + (lock ? " FOR UPDATE" : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setString(3, pageId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapPage(result)) : Optional.empty();
            }
        }
    }

    static Optional<PageRow> findPartition(Connection connection, String tenantId, String jobId,
            int stageId, int partitionId) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM artifact_build_pages "
                + "WHERE tenant_id=? AND job_id=? AND stage_id=? AND partition_id=? FOR UPDATE")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setInt(3, stageId);
            statement.setInt(4, partitionId);
            try (var result = statement.executeQuery()) {
                if (!result.next()) return Optional.empty();
                var page = mapPage(result);
                if (result.next()) throw new IllegalStateException("Durable partition has ambiguous page identity");
                return Optional.of(page);
            }
        }
    }

    static PageRow mapPage(ResultSet result) throws SQLException {
        Instant leaseExpiresAt = getInstant(result, "lease_expires_at");
        return new PageRow(
                result.getString("tenant_id"), result.getString("job_id"),
                result.getString("page_id"),
                result.getInt("stage_id"), result.getInt("partition_id"),
                result.getInt("page_ordinal"),
                GenerationPageKind.valueOf(result.getString("page_kind")),
                result.getString("state"),
                result.getInt("attempt_id"), result.getLong("fencing_token"),
                result.getString("lease_owner"), result.getLong("owner_epoch"),
                leaseExpiresAt,
                result.getInt("max_attempts"), result.getBytes("report_bytes"),
                result.getString("report_sha256"),
                result.getString("execution_owner_id") == null ? null
                        : new org.pangrova.spi.generation.execution.GenerationExecutionOwner(
                                result.getString("execution_owner_id"), result.getString("execution_owner_incarnation"),
                                result.getLong("execution_owner_token")));
    }

}
