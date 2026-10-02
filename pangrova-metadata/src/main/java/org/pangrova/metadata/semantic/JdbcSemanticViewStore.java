/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.metadata.semantic;

import org.pangrova.metadata.exception.MetastoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.pangrova.metadata.store.JdbcSupport.canonicalInstant;
import static org.pangrova.metadata.store.JdbcSupport.getInstant;
import static org.pangrova.metadata.store.JdbcSupport.setInstant;

/** PostgreSQL implementation of immutable Semantic View metadata and fenced publication. */
public final class JdbcSemanticViewStore implements SemanticViewStore {

    private final DataSource dataSource;

    public JdbcSemanticViewStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void createVersion(SemanticViewVersion version) {
        Objects.requireNonNull(version, "version must not be null");
        String sql = "INSERT INTO semantic_view_versions (tenant_id,catalog_name,schema_name,"
                + "view_name,version,owner_principal,definition_json,definition_sha256,"
                + "validation_state,validation_message,created_at) "
                + "VALUES (?,?,?,?,?,?,CAST(? AS jsonb),?,?,?,?)";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindId(statement, version.id(), 1);
            statement.setLong(5, version.version());
            statement.setString(6, version.ownerPrincipal());
            statement.setString(7, version.definitionJson());
            statement.setString(8, version.definitionSha256().toLowerCase(Locale.ROOT));
            statement.setString(9, version.validationState().name());
            statement.setString(10, version.validationMessage());
            setInstant(statement, 11, version.createdAt());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to create Semantic View version", failure);
        }
    }

    @Override
    public Optional<SemanticViewVersion> findVersion(SemanticViewId id, long version) {
        requireVersion(version);
        Objects.requireNonNull(id, "id must not be null");
        String sql = "SELECT * FROM semantic_view_versions WHERE tenant_id=? "
                + "AND catalog_name=? AND schema_name=? AND view_name=? AND version=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindId(statement, id, 1);
            statement.setLong(5, version);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapVersion(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to find Semantic View version", failure);
        }
    }

    @Override
    public List<SemanticViewVersion> listVersions(SemanticViewId id) {
        Objects.requireNonNull(id, "id must not be null");
        String sql = "SELECT * FROM semantic_view_versions WHERE tenant_id=? "
                + "AND catalog_name=? AND schema_name=? AND view_name=? ORDER BY version";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindId(statement, id, 1);
            try (ResultSet result = statement.executeQuery()) {
                List<SemanticViewVersion> versions = new ArrayList<>();
                while (result.next()) {
                    versions.add(mapVersion(result));
                }
                return List.copyOf(versions);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to list Semantic View versions", failure);
        }
    }

    @Override
    public List<SemanticViewVersionStatus> listVersionStatuses(
            String tenantId, String catalogName, String schemaName, int limit) {
        SemanticViewValidation.requireText(tenantId, "tenantId");
        SemanticViewValidation.requireText(catalogName, "catalogName");
        SemanticViewValidation.requireText(schemaName, "schemaName");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        String sql = "SELECT v.*,a.active_version,a.publication_epoch "
                + "FROM semantic_view_versions v LEFT JOIN semantic_view_active_versions a "
                + "ON a.tenant_id=v.tenant_id AND a.catalog_name=v.catalog_name "
                + "AND a.schema_name=v.schema_name AND a.view_name=v.view_name "
                + "WHERE v.tenant_id=? AND v.catalog_name=? AND v.schema_name=? "
                + "ORDER BY v.view_name,v.version DESC LIMIT ?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, catalogName);
            statement.setString(3, schemaName);
            statement.setInt(4, limit);
            try (ResultSet result = statement.executeQuery()) {
                List<SemanticViewVersionStatus> statuses = new ArrayList<>();
                while (result.next()) {
                    Long activeVersion = (Long) result.getObject("active_version");
                    statuses.add(new SemanticViewVersionStatus(
                            mapVersion(result),
                            activeVersion == null
                                    ? OptionalLong.empty()
                                    : OptionalLong.of(activeVersion),
                            activeVersion == null ? 0 : result.getLong("publication_epoch")));
                }
                return List.copyOf(statuses);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to list Semantic View status", failure);
        }
    }

    @Override
    public Optional<SemanticViewVersion> findActiveVersion(SemanticViewId id) {
        Objects.requireNonNull(id, "id must not be null");
        String sql = "SELECT v.* FROM semantic_view_active_versions a "
                + "JOIN semantic_view_versions v ON v.tenant_id=a.tenant_id "
                + "AND v.catalog_name=a.catalog_name AND v.schema_name=a.schema_name "
                + "AND v.view_name=a.view_name AND v.version=a.active_version "
                + "WHERE a.tenant_id=? AND a.catalog_name=? AND a.schema_name=? "
                + "AND a.view_name=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindId(statement, id, 1);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapVersion(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to find active Semantic View", failure);
        }
    }

    @Override
    public Optional<SemanticViewPublication> findPublication(SemanticViewId id) {
        Objects.requireNonNull(id, "id must not be null");
        String sql = "SELECT * FROM semantic_view_active_versions WHERE tenant_id=? "
                + "AND catalog_name=? AND schema_name=? AND view_name=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindId(statement, id, 1);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(new SemanticViewPublication(
                        id,
                        result.getLong("active_version"),
                        result.getLong("publication_epoch"),
                        result.getString("published_by"),
                        getInstant(result, "published_at")));
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to find Semantic View publication", failure);
        }
    }

    @Override
    public Optional<SemanticViewPublication> publishVersion(
            SemanticViewId id,
            long version,
            OptionalLong expectedPublicationEpoch,
            String publishedBy,
            Instant publishedAt) {
        Objects.requireNonNull(id, "id must not be null");
        requireVersion(version);
        Objects.requireNonNull(
                expectedPublicationEpoch, "expectedPublicationEpoch must not be null");
        if (expectedPublicationEpoch.isPresent()) {
            requireVersion(expectedPublicationEpoch.getAsLong());
        }
        SemanticViewValidation.requireText(publishedBy, "publishedBy");
        Objects.requireNonNull(publishedAt, "publishedAt must not be null");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                SemanticViewVersion candidate = findVersion(connection, id, version, true)
                        .orElseThrow(() -> new IllegalStateException(
                                "Semantic View version does not exist: " + version));
                if (candidate.validationState() != SemanticViewValidationState.VALIDATED) {
                    throw new IllegalStateException(
                            "Cannot publish an invalid Semantic View version: " + version);
                }
                OptionalLong publicationEpoch = expectedPublicationEpoch.isPresent()
                        ? updatePublication(
                                connection, id, version, expectedPublicationEpoch.getAsLong(),
                                publishedBy, publishedAt)
                        : insertInitialPublication(
                                connection, id, version, publishedBy, publishedAt);
                connection.commit();
                return publicationEpoch.isPresent()
                        ? Optional.of(new SemanticViewPublication(
                                id, version, publicationEpoch.getAsLong(),
                                publishedBy, canonicalInstant(publishedAt)))
                        : Optional.empty();
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to publish Semantic View version", failure);
        }
    }

    private static Optional<SemanticViewVersion> findVersion(
            Connection connection, SemanticViewId id, long version, boolean lock)
            throws SQLException {
        String sql = "SELECT * FROM semantic_view_versions WHERE tenant_id=? "
                + "AND catalog_name=? AND schema_name=? AND view_name=? AND version=?"
                + (lock ? " FOR SHARE" : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindId(statement, id, 1);
            statement.setLong(5, version);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapVersion(result)) : Optional.empty();
            }
        }
    }

    private static OptionalLong insertInitialPublication(
            Connection connection,
            SemanticViewId id,
            long version,
            String publishedBy,
            Instant publishedAt) throws SQLException {
        String sql = "INSERT INTO semantic_view_active_versions (tenant_id,catalog_name,"
                + "schema_name,view_name,active_version,publication_epoch,published_by,published_at) "
                + "VALUES (?,?,?,?,?,1,?,?) ON CONFLICT DO NOTHING "
                + "RETURNING publication_epoch";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindId(statement, id, 1);
            statement.setLong(5, version);
            statement.setString(6, publishedBy);
            setInstant(statement, 7, publishedAt);
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                        ? OptionalLong.of(result.getLong("publication_epoch"))
                        : OptionalLong.empty();
            }
        }
    }

    private static OptionalLong updatePublication(
            Connection connection,
            SemanticViewId id,
            long version,
            long expectedEpoch,
            String publishedBy,
            Instant publishedAt) throws SQLException {
        String sql = "UPDATE semantic_view_active_versions SET active_version=?, "
                + "publication_epoch=publication_epoch+1,published_by=?,published_at=? "
                + "WHERE tenant_id=? AND catalog_name=? AND schema_name=? AND view_name=? "
                + "AND publication_epoch=? RETURNING publication_epoch";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, version);
            statement.setString(2, publishedBy);
            setInstant(statement, 3, publishedAt);
            bindId(statement, id, 4);
            statement.setLong(8, expectedEpoch);
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                        ? OptionalLong.of(result.getLong("publication_epoch"))
                        : OptionalLong.empty();
            }
        }
    }

    private static SemanticViewVersion mapVersion(ResultSet result) throws SQLException {
        SemanticViewId id = new SemanticViewId(
                result.getString("tenant_id"),
                result.getString("catalog_name"),
                result.getString("schema_name"),
                result.getString("view_name"));
        return new SemanticViewVersion(
                id,
                result.getLong("version"),
                result.getString("owner_principal"),
                result.getString("definition_json"),
                result.getString("definition_sha256"),
                SemanticViewValidationState.valueOf(result.getString("validation_state")),
                result.getString("validation_message"),
                getInstant(result, "created_at"));
    }

    private static void bindId(
            PreparedStatement statement, SemanticViewId id, int parameter) throws SQLException {
        statement.setString(parameter, id.tenantId());
        statement.setString(parameter + 1, id.catalogName());
        statement.setString(parameter + 2, id.schemaName());
        statement.setString(parameter + 3, id.viewName());
    }

    private static void requireVersion(long version) {
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
    }

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }
}
