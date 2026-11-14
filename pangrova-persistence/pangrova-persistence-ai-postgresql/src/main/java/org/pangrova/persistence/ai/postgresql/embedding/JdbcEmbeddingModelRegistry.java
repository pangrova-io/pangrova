/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.persistence.ai.postgresql.embedding;

import org.pangrova.ai.control.exception.DimensionMismatchException;
import org.pangrova.ai.control.embedding.EmbeddingModelAlterTarget;
import org.pangrova.ai.control.embedding.EmbeddingModelRegistry;
import org.pangrova.ai.control.embedding.EmbeddingModelRevision;
import org.pangrova.ai.control.exception.ModelAlreadyExistsException;
import org.pangrova.ai.control.exception.ModelDropUnavailableException;
import org.pangrova.ai.control.exception.ModelNotFoundException;
import org.pangrova.ai.control.exception.VersionConflictException;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pangrova.common.api.annotation.ThreadSafe;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC-backed implementation of {@link EmbeddingModelRegistry} for durable
 * embedding model configuration persistence.
 *
 * <p>Survives coordinator restarts by persisting model descriptors in the shared
 * metadata database. Uses optimistic concurrency via a version column for ALTER
 * operations: if the current DB version does not match the expected version, a
 * {@link VersionConflictException} is thrown and no update is applied.</p>
 *
 * <p>Access requires current direct PostgreSQL roles. Each model stores its
 * administrative and invocation role policy; transient identity groups are not authority.</p>
 *
 * <p>Call {@link #validateSchema()} after the canonical metadata migrations to validate the backing table.</p>
 */
@ThreadSafe
public final class JdbcEmbeddingModelRegistry implements EmbeddingModelRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcEmbeddingModelRegistry.class);

    private static final String TABLE = "pangrova_embedding_models";

    private static final String INSERT_SQL =
            "INSERT INTO " + TABLE
                    + " (model_name, provider, endpoint, dimension, api_key_secret,"
                    + "  max_batch_size, max_rps, query_prefix, document_prefix, version,"
                    + "  deployment_version, deployment_signature_sha256, incarnation_id,"
                    + "  has_dependents, admin_role, usage_role)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_BY_NAME_SQL =
            "SELECT model_name, provider, endpoint, dimension, api_key_secret,"
                    + " max_batch_size, max_rps, query_prefix, document_prefix, version,"
                    + " deployment_version, deployment_signature_sha256, has_dependents"
                    + ", incarnation_id"
                    + " FROM " + TABLE + " WHERE model_name = ?";

    private static final String SELECT_ALL_SQL =
            "SELECT model_name, provider, endpoint, dimension, api_key_secret,"
                    + " max_batch_size, max_rps, query_prefix, document_prefix, version,"
                    + " deployment_version, deployment_signature_sha256, has_dependents"
                    + ", incarnation_id"
                    + " FROM " + TABLE;

    private static final String UPDATE_WITH_VERSION_SQL =
            "UPDATE " + TABLE
                    + " SET provider = ?, endpoint = ?, dimension = ?, api_key_secret = ?,"
                    + "     max_batch_size = ?, max_rps = ?, query_prefix = ?, document_prefix = ?,"
                    + "     version = ?, deployment_version = ?,"
                    + "     deployment_signature_sha256 = ?"
                    + " WHERE model_name = ? AND version = ? AND incarnation_id = ?";

    private static final String UPDATE_DEPENDENTS_SQL =
            "UPDATE " + TABLE + " SET has_dependents = ? WHERE model_name = ?";

    private final DataSource dataSource;
    private final String adminRole;
    private final String usageRole;

    /** Creates a registry whose new models explicitly require the given persistent roles. */
    public JdbcEmbeddingModelRegistry(DataSource dataSource, String adminRole, String usageRole) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource required");
        if (adminRole == null || adminRole.isBlank()) {
            throw new IllegalArgumentException("adminRole required");
        }
        if (usageRole == null || usageRole.isBlank()) {
            throw new IllegalArgumentException("usageRole required");
        }
        this.adminRole = adminRole;
        this.usageRole = usageRole;
    }

    /**
     * Validates the canonical migrated schema without creating or upgrading any table.
     * Missing columns fail closed; only the metadata Flyway migration chain may change this schema.
     */
    public void validateSchema() {
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            try (var schema = stmt.executeQuery("SELECT model_name,provider,endpoint,dimension,api_key_secret,"
                    + "max_batch_size,max_rps,query_prefix,document_prefix,version,deployment_version,"
                    + "deployment_signature_sha256,incarnation_id,has_dependents,admin_role,usage_role "
                    + "FROM " + TABLE + " WHERE FALSE")) {
                schema.getMetaData();
            }
            LOG.info("event=embedding_model_registry_schema_validated");
        } catch (SQLException e) {
            throw new IllegalStateException("Embedding model registry requires the canonical metadata migrations", e);
        }
    }

    @Override
    public void authorizeCreate(String name, UserIdentity user) {
        requireAdmin(user, "CREATE", name);
    }

    @Override
    public EmbeddingModelAlterTarget prepareAlter(String name, UserIdentity user) {
        requireAdmin(user, "ALTER", name);
        try (Connection conn = dataSource.getConnection()) {
            RegisteredModel model = findRecordByName(conn, name)
                    .orElseThrow(() -> new ModelNotFoundException(name));
            return model.alterTarget();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to prepare embedding model alteration: " + name, e);
        }
    }

    @Override
    public void create(EmbeddingModelDescriptor descriptor, UserIdentity user) {
        authorizeCreate(descriptor.name(), user);
        if (descriptor.version() != 0) {
            throw new IllegalArgumentException("New model version must be 0, got: " + descriptor.version());
        }
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(conn);
            EmbeddingModelRoleAuthority.requireRole(conn, user, adminRole, "CREATE", descriptor.name());
            insert(conn, descriptor, java.util.UUID.randomUUID().toString(), false);
            conn.commit();
        } catch (SQLException e) {
            if (isUniqueConstraintViolation(e)) {
                throw new ModelAlreadyExistsException(descriptor.name());
            }
            throw new IllegalStateException("Failed to create embedding model: " + descriptor.name(), e);
        }
        LOG.info("event=embedding_model_created name={} provider={} dimension={} user={}",
                descriptor.name(), descriptor.provider(), descriptor.dimension(), user.principal());
    }

    @Override
    public EmbeddingModelDescriptor alter(
            EmbeddingModelAlterTarget target,
            EmbeddingModelDescriptor replacement, UserIdentity user) {
        String name = target.descriptor().name();
        requireAdmin(user, "ALTER", name);
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(conn);
            EmbeddingModelRoleAuthority.requireModelRole(conn, user, name, true, "ALTER");
            RegisteredModel current = findRecordByName(conn, name)
                    .orElseThrow(() -> new ModelNotFoundException(name));
            EmbeddingModelRevision.requireValidReplacement(
                    name, current.descriptor(), replacement, target.descriptor().version());
            if (!current.incarnationId().equals(target.incarnationId())) {
                throw new VersionConflictException(
                        name, target.descriptor().version(), current.descriptor().version());
            }
            int rows = executeUpdateWithVersion(conn, replacement, target);
            if (rows == 0) {
                RegisteredModel actual = findRecordByName(conn, name).orElse(null);
                int actualVersion = actual != null ? actual.descriptor().version() : -1;
                throw new VersionConflictException(
                        name, target.descriptor().version(), actualVersion);
            }
            LOG.info("event=embedding_model_altered name={} version={}->{} user={}",
                    name, target.descriptor().version(), replacement.version(), user.principal());
            conn.commit();
            return replacement;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to alter embedding model: " + name, e);
        }
    }

    @Override
    public void drop(String name, boolean cascade, UserIdentity user) {
        requireAdmin(user, "DROP", name);
        try (Connection conn = dataSource.getConnection()) {
            findRecordByName(conn, name)
                    .orElseThrow(() -> new ModelNotFoundException(name));
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to drop embedding model: " + name, e);
        }
        throw new ModelDropUnavailableException(name);
    }

    @Override
    public Optional<EmbeddingModelDescriptor> find(String name, UserIdentity user) {
        try (Connection conn = dataSource.getConnection()) {
            var found = findRecordByName(conn, name);
            if (found.isPresent()) {
                EmbeddingModelRoleAuthority.requireModelRole(conn, user, name, false, "FIND");
            }
            return found.map(RegisteredModel::descriptor);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to find embedding model: " + name, e);
        }
    }

    @Override
    public List<EmbeddingModelDescriptor> listAll(UserIdentity user) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALL_SQL);
             ResultSet rs = ps.executeQuery()) {
            var result = new ArrayList<EmbeddingModelDescriptor>();
            while (rs.next()) {
                if (EmbeddingModelRoleAuthority.hasModelRole(conn, user, rs.getString("model_name"), false)) {
                    result.add(rowToDescriptor(rs));
                }
            }
            return List.copyOf(result);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list embedding models", e);
        }
    }

    @Override
    public void validateDimension(String modelName, int columnDimension) {
        try (Connection conn = dataSource.getConnection()) {
            EmbeddingModelDescriptor descriptor = findRecordByName(conn, modelName)
                    .map(RegisteredModel::descriptor)
                    .orElseThrow(() -> new ModelNotFoundException(modelName));
            if (descriptor.dimension() != columnDimension) {
                throw new DimensionMismatchException(modelName, descriptor.dimension(), columnDimension);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to validate dimension for model: " + modelName, e);
        }
    }

    /**
     * Registers a model name as having dependent auto-embedding columns.
     */
    public void registerDependency(String modelName) {
        setDependents(modelName, true);
    }

    /**
     * Removes a model dependency registration.
     */
    public void removeDependency(String modelName) {
        setDependents(modelName, false);
    }

    // ── Internal helpers ──

    private Optional<RegisteredModel> findRecordByName(Connection conn, String name)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SELECT_BY_NAME_SQL)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new RegisteredModel(
                            rowToDescriptor(rs), rs.getString("incarnation_id")));
                }
                return Optional.empty();
            }
        }
    }

    private void insert(
            Connection conn, EmbeddingModelDescriptor d,
            String incarnationId, boolean hasDependents)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            int parameter = 1;
            ps.setString(parameter++, d.name());
            ps.setString(parameter++, d.provider());
            ps.setString(parameter++, d.endpoint());
            ps.setInt(parameter++, d.dimension());
            ps.setString(parameter++, d.apiKeySecret());
            ps.setInt(parameter++, d.maxBatchSize());
            ps.setInt(parameter++, d.maxRequestsPerSecond());
            ps.setString(parameter++, d.queryPrefix());
            ps.setString(parameter++, d.documentPrefix());
            ps.setInt(parameter++, d.version());
            ps.setString(parameter++, d.deploymentVersion());
            ps.setString(parameter++, d.deploymentSignatureSha256());
            ps.setString(parameter++, incarnationId);
            ps.setBoolean(parameter++, hasDependents);
            ps.setString(parameter++, adminRole);
            ps.setString(parameter, usageRole);
            ps.executeUpdate();
        }
    }

    private static boolean isUniqueConstraintViolation(SQLException failure) {
        SQLException current = failure;
        while (current != null) {
            String sqlState = current.getSQLState();
            if ("23505".equals(sqlState) || "23000".equals(sqlState)) {
                return true;
            }
            current = current.getNextException();
        }
        return false;
    }

    private int executeUpdateWithVersion(
            Connection conn,
            EmbeddingModelDescriptor updated,
            EmbeddingModelAlterTarget target) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPDATE_WITH_VERSION_SQL)) {
            int parameter = 1;
            ps.setString(parameter++, updated.provider());
            ps.setString(parameter++, updated.endpoint());
            ps.setInt(parameter++, updated.dimension());
            ps.setString(parameter++, updated.apiKeySecret());
            ps.setInt(parameter++, updated.maxBatchSize());
            ps.setInt(parameter++, updated.maxRequestsPerSecond());
            ps.setString(parameter++, updated.queryPrefix());
            ps.setString(parameter++, updated.documentPrefix());
            ps.setInt(parameter++, updated.version());
            ps.setString(parameter++, updated.deploymentVersion());
            ps.setString(parameter++, updated.deploymentSignatureSha256());
            ps.setString(parameter++, updated.name());
            ps.setInt(parameter++, target.descriptor().version());
            ps.setString(parameter++, target.incarnationId());
            return ps.executeUpdate();
        }
    }

    private void setDependents(String modelName, boolean hasDependents) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_DEPENDENTS_SQL)) {
            ps.setBoolean(1, hasDependents);
            ps.setString(2, modelName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to update dependents flag for model: " + modelName, e);
        }
    }

    private static EmbeddingModelDescriptor rowToDescriptor(ResultSet rs) throws SQLException {
        return new EmbeddingModelDescriptor(
                rs.getString("model_name"),
                new EmbeddingModelDescriptor.ProviderRoute(
                        rs.getString("provider"),
                        rs.getString("endpoint"),
                        rs.getString("api_key_secret")),
                new EmbeddingModelDescriptor.EmbeddingContract(
                        rs.getInt("dimension"),
                        rs.getString("query_prefix"),
                        rs.getString("document_prefix")),
                new EmbeddingModelDescriptor.ServingLimits(
                        rs.getInt("max_batch_size"), rs.getInt("max_rps")),
                rs.getInt("version"),
                new EmbeddingModelDescriptor.DeploymentFence(
                        rs.getString("deployment_version"),
                        rs.getString("deployment_signature_sha256")));
    }

    private record RegisteredModel(
            EmbeddingModelDescriptor descriptor,
            String incarnationId) {

        private EmbeddingModelAlterTarget alterTarget() {
            return new EmbeddingModelAlterTarget(descriptor, incarnationId);
        }
    }

    private void requireAdmin(UserIdentity user, String operation, String modelName) {
        try (var connection = dataSource.getConnection()) {
            if ("CREATE".equals(operation)) {
                EmbeddingModelRoleAuthority.requireRole(connection, user, adminRole, operation, modelName);
            } else {
                EmbeddingModelRoleAuthority.requireModelRole(connection, user, modelName, true, operation);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot read current model administration authority", failure);
        }
    }

}
