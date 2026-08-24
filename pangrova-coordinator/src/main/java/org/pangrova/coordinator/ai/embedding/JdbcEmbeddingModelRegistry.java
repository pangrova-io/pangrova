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
package org.pangrova.coordinator.ai.embedding;

import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
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
 * <p>RBAC enforcement mirrors {@link ConcurrentMapEmbeddingModelRegistry}: ADMIN
 * group required for mutating operations; USAGE group required for reads.</p>
 *
 * <p>Call {@link #initSchema()} once at bootstrap to create the backing table.</p>
 */
@ThreadSafe
public final class JdbcEmbeddingModelRegistry implements EmbeddingModelRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcEmbeddingModelRegistry.class);

    private static final String TABLE = "pangrova_embedding_models";

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS " + TABLE + " ("
                    + "model_name      VARCHAR(256) NOT NULL, "
                    + "provider        VARCHAR(128) NOT NULL, "
                    + "endpoint        VARCHAR(512) NOT NULL, "
                    + "dimension       INT          NOT NULL, "
                    + "api_key_secret  VARCHAR(512) NOT NULL, "
                    + "max_batch_size  INT          NOT NULL, "
                    + "max_rps         INT          NOT NULL, "
                    + "query_prefix    TEXT         NOT NULL, "
                    + "document_prefix TEXT         NOT NULL, "
                    + "version         INT          NOT NULL DEFAULT 0, "
                    + "has_dependents  BOOLEAN      NOT NULL DEFAULT FALSE, "
                    + "PRIMARY KEY (model_name)"
                    + ")";

    private static final String INSERT_SQL =
            "INSERT INTO " + TABLE
                    + " (model_name, provider, endpoint, dimension, api_key_secret,"
                    + "  max_batch_size, max_rps, query_prefix, document_prefix, version, has_dependents)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_BY_NAME_SQL =
            "SELECT model_name, provider, endpoint, dimension, api_key_secret,"
                    + " max_batch_size, max_rps, query_prefix, document_prefix, version, has_dependents"
                    + " FROM " + TABLE + " WHERE model_name = ?";

    private static final String SELECT_ALL_SQL =
            "SELECT model_name, provider, endpoint, dimension, api_key_secret,"
                    + " max_batch_size, max_rps, query_prefix, document_prefix, version, has_dependents"
                    + " FROM " + TABLE;

    private static final String UPDATE_WITH_VERSION_SQL =
            "UPDATE " + TABLE
                    + " SET provider = ?, endpoint = ?, dimension = ?, api_key_secret = ?,"
                    + "     max_batch_size = ?, max_rps = ?, query_prefix = ?, document_prefix = ?,"
                    + "     version = version + 1"
                    + " WHERE model_name = ? AND version = ?";

    private static final String DELETE_SQL =
            "DELETE FROM " + TABLE + " WHERE model_name = ?";

    private static final String SELECT_HAS_DEPENDENTS_SQL =
            "SELECT has_dependents FROM " + TABLE + " WHERE model_name = ?";

    private static final String UPDATE_DEPENDENTS_SQL =
            "UPDATE " + TABLE + " SET has_dependents = ? WHERE model_name = ?";

    /** JDBC parameter index of the has_dependents column in the insert statement. */
    private static final int HAS_DEPENDENTS_PARAM_INDEX = 11;

    private final DataSource dataSource;
    private final String adminGroup;
    private final String usageGroup;

    /** Creates a JDBC-backed registry enforcing the given admin and usage access groups. */
    public JdbcEmbeddingModelRegistry(DataSource dataSource, String adminGroup, String usageGroup) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource required");
        if (adminGroup == null || adminGroup.isBlank()) {
            throw new IllegalArgumentException("adminGroup required");
        }
        if (usageGroup == null || usageGroup.isBlank()) {
            throw new IllegalArgumentException("usageGroup required");
        }
        this.adminGroup = adminGroup;
        this.usageGroup = usageGroup;
    }

    /**
     * Creates the backing table if it does not already exist.
     * Must be called once at bootstrap before any other method.
     */
    public void initSchema() {
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
            LOG.info("event=embedding_model_registry_schema_initialized");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize embedding model registry schema", e);
        }
    }

    @Override
    public void create(EmbeddingModelDescriptor descriptor, UserIdentity user) {
        requireAdmin(user, "CREATE", descriptor.name());
        if (descriptor.version() != 0) {
            throw new IllegalArgumentException("New model version must be 0, got: " + descriptor.version());
        }
        try (Connection conn = dataSource.getConnection()) {
            if (findByName(conn, descriptor.name()).isPresent()) {
                throw new ModelAlreadyExistsException(descriptor.name());
            }
            upsert(conn, descriptor, false);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create embedding model: " + descriptor.name(), e);
        }
        LOG.info("event=embedding_model_created name={} provider={} dimension={} user={}",
                descriptor.name(), descriptor.provider(), descriptor.dimension(), user.principal());
    }

    @Override
    public EmbeddingModelDescriptor alter(String name, ModelUpdateRequest update,
                                          int expectedVersion, UserIdentity user) {
        requireAdmin(user, "ALTER", name);
        if (!update.hasUpdates()) {
            throw new IllegalArgumentException("No fields to update in ALTER request");
        }
        try (Connection conn = dataSource.getConnection()) {
            EmbeddingModelDescriptor current = findByName(conn, name)
                    .orElseThrow(() -> new ModelNotFoundException(name));
            if (current.version() != expectedVersion) {
                throw new VersionConflictException(name, expectedVersion, current.version());
            }
            EmbeddingModelDescriptor updated = applyUpdate(current, update);
            int rows = executeUpdateWithVersion(conn, updated, expectedVersion);
            if (rows == 0) {
                EmbeddingModelDescriptor actual = findByName(conn, name).orElse(null);
                int actualVersion = actual != null ? actual.version() : -1;
                throw new VersionConflictException(name, expectedVersion, actualVersion);
            }
            LOG.info("event=embedding_model_altered name={} version={}->{} user={}",
                    name, expectedVersion, updated.version(), user.principal());
            return updated;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to alter embedding model: " + name, e);
        }
    }

    @Override
    public void drop(String name, boolean cascade, UserIdentity user) {
        requireAdmin(user, "DROP", name);
        try (Connection conn = dataSource.getConnection()) {
            findByName(conn, name).orElseThrow(() -> new ModelNotFoundException(name));
            if (!cascade && hasDependents(conn, name)) {
                throw new ModelInUseException(name);
            }
            try (PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
                ps.setString(1, name);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to drop embedding model: " + name, e);
        }
        LOG.info("event=embedding_model_dropped name={} cascade={} user={}", name, cascade, user.principal());
    }

    @Override
    public Optional<EmbeddingModelDescriptor> find(String name, UserIdentity user) {
        requireUsage(user, "FIND", name);
        try (Connection conn = dataSource.getConnection()) {
            return findByName(conn, name);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to find embedding model: " + name, e);
        }
    }

    @Override
    public List<EmbeddingModelDescriptor> listAll(UserIdentity user) {
        if (!hasUsagePrivilege(user)) {
            return List.of();
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALL_SQL);
             ResultSet rs = ps.executeQuery()) {
            var result = new ArrayList<EmbeddingModelDescriptor>();
            while (rs.next()) {
                result.add(rowToDescriptor(rs));
            }
            return List.copyOf(result);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list embedding models", e);
        }
    }

    @Override
    public void validateDimension(String modelName, int columnDimension) {
        try (Connection conn = dataSource.getConnection()) {
            EmbeddingModelDescriptor descriptor = findByName(conn, modelName)
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

    private boolean hasDependents(Connection conn, String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SELECT_HAS_DEPENDENTS_SQL)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean("has_dependents");
            }
        }
    }

    private Optional<EmbeddingModelDescriptor> findByName(Connection conn, String name)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SELECT_BY_NAME_SQL)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rowToDescriptor(rs));
                }
                return Optional.empty();
            }
        }
    }

    private void upsert(Connection conn, EmbeddingModelDescriptor d, boolean hasDependents)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            ps.setString(1, d.name());
            ps.setString(2, d.provider());
            ps.setString(3, d.endpoint());
            ps.setInt(4, d.dimension());
            ps.setString(5, d.apiKeySecret());
            ps.setInt(6, d.maxBatchSize());
            ps.setInt(7, d.maxRequestsPerSecond());
            ps.setString(8, d.queryPrefix());
            ps.setString(9, d.documentPrefix());
            ps.setInt(10, d.version());
            ps.setBoolean(HAS_DEPENDENTS_PARAM_INDEX, hasDependents);
            ps.executeUpdate();
        }
    }

    private int executeUpdateWithVersion(Connection conn, EmbeddingModelDescriptor updated,
                                          int expectedVersion) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPDATE_WITH_VERSION_SQL)) {
            ps.setString(1, updated.provider());
            ps.setString(2, updated.endpoint());
            ps.setInt(3, updated.dimension());
            ps.setString(4, updated.apiKeySecret());
            ps.setInt(5, updated.maxBatchSize());
            ps.setInt(6, updated.maxRequestsPerSecond());
            ps.setString(7, updated.queryPrefix());
            ps.setString(8, updated.documentPrefix());
            ps.setString(9, updated.name());
            ps.setInt(10, expectedVersion);
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
                rs.getString("provider"),
                rs.getString("endpoint"),
                rs.getInt("dimension"),
                rs.getString("api_key_secret"),
                rs.getInt("max_batch_size"),
                rs.getInt("max_rps"),
                rs.getString("query_prefix"),
                rs.getString("document_prefix"),
                rs.getInt("version"));
    }

    private static EmbeddingModelDescriptor applyUpdate(EmbeddingModelDescriptor current,
                                                         ModelUpdateRequest update) {
        return new EmbeddingModelDescriptor(
                current.name(),
                current.provider(),
                update.endpoint().orElse(current.endpoint()),
                current.dimension(),
                update.apiKeySecret().orElse(current.apiKeySecret()),
                update.maxBatchSize().orElse(current.maxBatchSize()),
                update.maxRequestsPerSecond().orElse(current.maxRequestsPerSecond()),
                update.queryPrefix().orElse(current.queryPrefix()),
                update.documentPrefix().orElse(current.documentPrefix()),
                current.version() + 1);
    }

    private void requireAdmin(UserIdentity user, String operation, String modelName) {
        if (!user.groups().contains(adminGroup)) {
            throw new EmbeddingRegistryAccessDeniedException(user.principal(), operation, modelName);
        }
    }

    private void requireUsage(UserIdentity user, String operation, String modelName) {
        if (!hasUsagePrivilege(user)) {
            throw new EmbeddingRegistryAccessDeniedException(user.principal(), operation, modelName);
        }
    }

    private boolean hasUsagePrivilege(UserIdentity user) {
        return user.groups().contains(usageGroup) || user.groups().contains(adminGroup);
    }
}
