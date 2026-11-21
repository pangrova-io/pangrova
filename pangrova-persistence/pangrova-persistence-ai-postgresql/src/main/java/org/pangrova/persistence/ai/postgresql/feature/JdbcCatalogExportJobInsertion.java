package org.pangrova.persistence.ai.postgresql.feature;

import org.pangrova.ai.control.feature.CatalogExportRequest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** Inserts or validates the immutable catalog-to-artifact export link. */
final class JdbcCatalogExportJobInsertion {

    private static final int REQUESTED_BY_PARAMETER = 11;
    private static final int REQUESTED_AT_PARAMETER = 12;

    private JdbcCatalogExportJobInsertion() {}

    /** Creates an export link, or validates an idempotent replay of the same link. */
    static void insert(Connection connection, CatalogExportRequest request)
            throws SQLException {
        String insert = "INSERT INTO catalog_export_requests (tenant_id,object_kind,"
                + "catalog_name,schema_name,object_name,object_version,generation,artifact_id,"
                + "job_id,dependency_sha256,requested_by,requested_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (tenant_id,object_kind,"
                + "catalog_name,schema_name,object_name,object_version,generation) DO NOTHING";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            bind(statement, request);
            if (statement.executeUpdate() == 1) {
                return;
            }
        }
        requireSameExistingLink(connection, request);
    }

    private static void requireSameExistingLink(
            Connection connection, CatalogExportRequest request) throws SQLException {
        String select = "SELECT artifact_id,job_id,dependency_sha256,requested_by "
                + "FROM catalog_export_requests WHERE tenant_id=? AND object_kind=? "
                + "AND catalog_name=? AND schema_name=? AND object_name=? "
                + "AND object_version=? AND generation=? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(select)) {
            statement.setString(1, request.tenantId());
            statement.setString(2, request.objectKind().name());
            statement.setString(3, request.catalogName());
            statement.setString(4, request.schemaName());
            statement.setString(5, request.objectName());
            statement.setLong(6, request.objectVersion());
            statement.setString(7, request.generation());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()
                        || !request.artifactId().equals(result.getString("artifact_id"))
                        || !request.jobId().equals(result.getString("job_id"))
                        || !request.dependencySha256().equalsIgnoreCase(
                                result.getString("dependency_sha256"))
                        || !request.requestedBy().equals(result.getString("requested_by"))) {
                    throw new IllegalStateException(
                            "Export generation already has another artifact job");
                }
            }
        }
    }

    private static void bind(PreparedStatement statement, CatalogExportRequest request)
            throws SQLException {
        statement.setString(1, request.tenantId());
        statement.setString(2, request.objectKind().name());
        statement.setString(3, request.catalogName());
        statement.setString(4, request.schemaName());
        statement.setString(5, request.objectName());
        statement.setLong(6, request.objectVersion());
        statement.setString(7, request.generation());
        statement.setString(8, request.artifactId());
        statement.setString(9, request.jobId());
        statement.setString(10, request.dependencySha256().toLowerCase(Locale.ROOT));
        statement.setString(REQUESTED_BY_PARAMETER, request.requestedBy());
        setInstant(statement, REQUESTED_AT_PARAMETER, request.requestedAt());
    }
}
