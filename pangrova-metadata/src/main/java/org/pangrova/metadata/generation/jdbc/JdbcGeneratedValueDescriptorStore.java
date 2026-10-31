package org.pangrova.metadata.generation.jdbc;

import org.pangrova.metadata.generation.GeneratedValueBuildDescriptor;
import org.pangrova.metadata.exception.MetastoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;

import static org.pangrova.common.jdbc.JdbcSupport.getInstant;
import static org.pangrova.common.jdbc.JdbcSupport.setInstant;

/** JDBC persistence and fencing for generated-value descriptor targets. */
@org.pangrova.common.api.annotation.Internal
public final class JdbcGeneratedValueDescriptorStore {

    private final DataSource dataSource;

    JdbcGeneratedValueDescriptorStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    void register(GeneratedValueBuildDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                registerWithinTransaction(connection, descriptor);
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            if (refreshAfterConcurrentInsert(descriptor, failure)) {
                return;
            }
            throw new MetastoreException("Register generated-value artifact failed", failure);
        }
    }

    /** Registers the descriptor using the caller-owned submission transaction; does not commit or open connections. */
    public static void registerWithinTransaction(Connection connection, GeneratedValueBuildDescriptor descriptor)
            throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("Generated descriptor registration requires its enclosing transaction");
        }
        GeneratedDescriptor existing = lock(connection, descriptor.tenantId(), descriptor.artifactId());
        if (existing == null) {
            insert(connection, descriptor);
        } else {
            requireSameIdentity(existing, descriptor);
            updateTarget(connection, descriptor);
        }
    }

    private boolean refreshAfterConcurrentInsert(
            GeneratedValueBuildDescriptor descriptor, SQLException original) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                GeneratedDescriptor existing = lock(
                        connection, descriptor.tenantId(), descriptor.artifactId());
                if (existing == null) {
                    connection.rollback();
                    return false;
                }
                requireSameIdentity(existing, descriptor);
                updateTarget(connection, descriptor);
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException retryFailure) {
            original.addSuppressed(retryFailure);
            return false;
        }
    }

    static GeneratedDescriptor lock(
            Connection connection, String tenantId, String artifactId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT source_id,catalog_name,schema_name,table_name,target_column,"
                        + "target_generation,target_source_snapshot,publication_epoch,updated_at "
                        + "FROM generated_value_artifacts "
                        + "WHERE tenant_id=? AND artifact_id=? FOR UPDATE")) {
            statement.setString(1, tenantId);
            statement.setString(2, artifactId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return null;
                }
                return new GeneratedDescriptor(
                        tenantId, artifactId, result.getString(1), result.getString(2),
                        result.getString(3), result.getString(4), result.getString(5),
                        result.getString(6), result.getString(7), result.getLong(8),
                        getInstant(result, 9));
            }
        }
    }

    private static void insert(
            Connection connection, GeneratedValueBuildDescriptor descriptor)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO generated_value_artifacts "
                        + "(tenant_id,artifact_id,source_id,catalog_name,schema_name,"
                        + "table_name,target_column,"
                        + "definition_sha256,target_generation,target_source_snapshot,"
                        + "publication_epoch,created_at,updated_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            int parameter = 1;
            statement.setString(parameter++, descriptor.tenantId());
            statement.setString(parameter++, descriptor.artifactId());
            statement.setString(parameter++, descriptor.sourceId());
            statement.setString(parameter++, descriptor.catalogName());
            statement.setString(parameter++, descriptor.schemaName());
            statement.setString(parameter++, descriptor.tableName());
            statement.setString(parameter++, descriptor.targetColumn());
            statement.setString(parameter++, descriptor.definitionSha256().toLowerCase(
                    java.util.Locale.ROOT));
            statement.setString(parameter++, descriptor.targetGeneration());
            statement.setString(parameter++, descriptor.targetSourceSnapshot());
            statement.setLong(parameter++, 0L);
            setInstant(statement, parameter++, descriptor.updatedAt());
            setInstant(statement, parameter, descriptor.updatedAt());
            statement.executeUpdate();
        }
    }

    private static void requireSameIdentity(
            GeneratedDescriptor existing, GeneratedValueBuildDescriptor descriptor) {
        if (!existing.sourceId().equals(descriptor.sourceId())
                || !existing.catalogName().equals(descriptor.catalogName())
                || !existing.schemaName().equals(descriptor.schemaName())
                || !existing.tableName().equals(descriptor.tableName())
                || !existing.targetColumn().equalsIgnoreCase(descriptor.targetColumn())) {
            throw new IllegalStateException(
                    "Generated-value artifact identity collides with another table or column");
        }
    }

    private static void updateTarget(
            Connection connection, GeneratedValueBuildDescriptor descriptor)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE generated_value_artifacts SET definition_sha256=?,"
                        + "target_generation=?,target_source_snapshot=?,updated_at=? "
                        + "WHERE tenant_id=? AND artifact_id=?")) {
            statement.setString(1, descriptor.definitionSha256().toLowerCase(
                    java.util.Locale.ROOT));
            statement.setString(2, descriptor.targetGeneration());
            statement.setString(3, descriptor.targetSourceSnapshot());
            setInstant(statement, 4, descriptor.updatedAt());
            statement.setString(5, descriptor.tenantId());
            statement.setString(6, descriptor.artifactId());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Generated-value descriptor update was lost");
            }
        }
    }

    static boolean shouldActivate(
            GeneratedDescriptor descriptor, GenerationPublicationJob candidate) {
        return descriptor.targetGeneration().equals(candidate.generation())
                && descriptor.targetSourceSnapshot().equals(candidate.sourceSnapshot());
    }

    static void activate(
            Connection connection, GeneratedDescriptor descriptor,
            GenerationPublicationJob job, long publicationEpoch, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE generated_value_artifacts SET active_generation=?,"
                        + "active_source_snapshot=?,publication_epoch=?,updated_at=? "
                        + "WHERE tenant_id=? AND artifact_id=? "
                        + "AND publication_epoch=? AND updated_at=?")) {
            statement.setString(1, job.generation());
            statement.setString(2, job.sourceSnapshot());
            bindEpochFence(statement, descriptor, publicationEpoch, now, 3);
            requireUpdated(statement, "Generated-value activation fence was lost");
        }
    }

    static void advanceEpoch(
            Connection connection, GeneratedDescriptor descriptor,
            long publicationEpoch, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE generated_value_artifacts SET publication_epoch=?,updated_at=? "
                        + "WHERE tenant_id=? AND artifact_id=? "
                        + "AND publication_epoch=? AND updated_at=?")) {
            bindEpochFence(statement, descriptor, publicationEpoch, now, 1);
            requireUpdated(statement,
                    "Generated-value publication epoch fence was lost");
        }
    }

    private static void bindEpochFence(
            PreparedStatement statement, GeneratedDescriptor descriptor,
            long publicationEpoch, Instant now, int first) throws SQLException {
        statement.setLong(first, publicationEpoch);
        setInstant(statement, first + 1, now);
        statement.setString(first + 2, descriptor.tenantId());
        statement.setString(first + 3, descriptor.artifactId());
        statement.setLong(first + 4, descriptor.publicationEpoch());
        setInstant(statement, first + 5, descriptor.updatedAt());
    }

    private static void requireUpdated(PreparedStatement statement, String message)
            throws SQLException {
        if (statement.executeUpdate() != 1) {
            throw new IllegalStateException(message);
        }
    }

    private static void rollback(Connection connection, Exception original) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }

    record GeneratedDescriptor(
            String tenantId,
            String artifactId,
            String sourceId,
            String catalogName,
            String schemaName,
            String tableName,
            String targetColumn,
            String targetGeneration,
            String targetSourceSnapshot,
            long publicationEpoch,
            Instant updatedAt) {}
}
