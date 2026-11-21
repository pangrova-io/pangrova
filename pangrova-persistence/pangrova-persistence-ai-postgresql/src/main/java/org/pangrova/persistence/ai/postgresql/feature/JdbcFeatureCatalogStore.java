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
package org.pangrova.persistence.ai.postgresql.feature;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import org.pangrova.ai.control.enums.FeatureSplitKind;
import org.pangrova.ai.control.enums.CatalogExportKind;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.ai.control.exception.AiControlException;
import org.pangrova.ai.control.feature.FeatureCatalogStore;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.ai.control.feature.CatalogExportRequest;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.pangrova.common.util.Preconditions.requireText;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.canonicalInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setJson;

/** PostgreSQL implementation of the governed feature and training catalog. */
public final class JdbcFeatureCatalogStore implements FeatureCatalogStore {

    private static final ObjectMapper MAPPER = JsonMapperFactory.shared();
    private static final int FEATURE_CREATED_AT_PARAMETER = 11;
    private static final int TRAINING_SPLIT_KIND_PARAMETER = 11;
    private static final int TRAINING_SPLIT_SEED_PARAMETER = 12;
    private static final int TRAINING_CREATED_AT_PARAMETER = 13;

    private final DataSource dataSource;

    public JdbcFeatureCatalogStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void createFeatureView(FeatureViewVersion version) {
        Objects.requireNonNull(version, "version must not be null");
        String sql = "INSERT INTO feature_view_versions (tenant_id,catalog_name,schema_name,"
                + "feature_view_name,version,owner_principal,definition_json,definition_sha256,"
                + "source_plan,source_plan_sha256,created_at) VALUES (?,?,?,?,?,?,?, ?,?,?,?)";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindFeatureId(statement, version.id(), 1);
            statement.setLong(5, version.version());
            statement.setString(6, version.ownerPrincipal());
            setJson(statement, 7, version.definitionJson());
            statement.setString(8, version.definitionSha256().toLowerCase(Locale.ROOT));
            statement.setBytes(9, version.sourcePlan());
            statement.setString(10, version.sourcePlanSha256().toLowerCase(Locale.ROOT));
            setInstant(statement, FEATURE_CREATED_AT_PARAMETER, version.createdAt());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw new AiControlException("Failed to create Feature View version", failure);
        }
    }

    @Override
    public Optional<FeatureViewVersion> findFeatureView(FeatureViewId id, long version) {
        requireVersion(version);
        Objects.requireNonNull(id, "id must not be null");
        String sql = "SELECT * FROM feature_view_versions WHERE tenant_id=? "
                + "AND catalog_name=? AND schema_name=? AND feature_view_name=? AND version=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindFeatureId(statement, id, 1);
            statement.setLong(5, version);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapFeature(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new AiControlException("Failed to find Feature View version", failure);
        }
    }

    @Override
    public Optional<FeaturePublication> publishFeatureView(
            FeatureViewId id,
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
        requireText(publishedBy, "publishedBy");
        Objects.requireNonNull(publishedAt, "publishedAt must not be null");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                requireFeatureExists(connection, id, version);
                OptionalLong epoch = expectedPublicationEpoch.isPresent()
                        ? updatePublication(connection, id, version,
                                expectedPublicationEpoch.getAsLong(), publishedBy, publishedAt)
                        : insertPublication(connection, id, version, publishedBy, publishedAt);
                connection.commit();
                return epoch.isPresent()
                        ? Optional.of(new FeaturePublication(
                                id, version, epoch.getAsLong(), publishedBy,
                                canonicalInstant(publishedAt)))
                        : Optional.empty();
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new AiControlException("Failed to publish Feature View version", failure);
        }
    }

    @Override
    public void createTrainingDataset(TrainingDatasetVersion version) {
        Objects.requireNonNull(version, "version must not be null");
        String sql = "INSERT INTO training_dataset_versions (tenant_id,catalog_name,schema_name,"
                + "dataset_name,version,owner_principal,definition_json,definition_sha256,"
                + "normalized_plan,normalized_plan_sha256,split_kind,split_seed,created_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindDatasetId(statement, version.id(), 1);
            statement.setLong(5, version.version());
            statement.setString(6, version.ownerPrincipal());
            setJson(statement, 7, version.definitionJson());
            statement.setString(8, version.definitionSha256().toLowerCase(Locale.ROOT));
            statement.setBytes(9, version.normalizedPlan());
            statement.setString(10, version.normalizedPlanSha256().toLowerCase(Locale.ROOT));
            statement.setString(TRAINING_SPLIT_KIND_PARAMETER, version.splitKind().name());
            statement.setLong(TRAINING_SPLIT_SEED_PARAMETER, version.splitSeed());
            setInstant(statement, TRAINING_CREATED_AT_PARAMETER, version.createdAt());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw new AiControlException("Failed to create Training Dataset version", failure);
        }
    }

    @Override
    public Optional<TrainingDatasetVersion> findTrainingDataset(
            TrainingDatasetId id, long version) {
        requireVersion(version);
        Objects.requireNonNull(id, "id must not be null");
        String sql = "SELECT * FROM training_dataset_versions WHERE tenant_id=? "
                + "AND catalog_name=? AND schema_name=? AND dataset_name=? AND version=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindDatasetId(statement, id, 1);
            statement.setLong(5, version);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapDataset(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new AiControlException("Failed to find Training Dataset version", failure);
        }
    }

    @Override
    public Optional<CatalogExportRequest> findExport(
            String tenantId, CatalogExportKind kind, String catalogName,
            String schemaName, String objectName, long version, String generation) {
        requireText(tenantId, "tenantId");
        Objects.requireNonNull(kind, "kind must not be null");
        requireText(catalogName, "catalogName");
        requireText(schemaName, "schemaName");
        requireText(objectName, "objectName");
        requireVersion(version);
        requireText(generation, "generation");
        String sql = "SELECT * FROM catalog_export_requests WHERE tenant_id=? AND object_kind=? "
                + "AND catalog_name=? AND schema_name=? AND object_name=? "
                + "AND object_version=? AND generation=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, kind.name());
            statement.setString(3, catalogName);
            statement.setString(4, schemaName);
            statement.setString(5, objectName);
            statement.setLong(6, version);
            statement.setString(7, generation);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapExport(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new AiControlException("Failed to find catalog export", failure);
        }
    }

    private static FeatureViewVersion mapFeature(ResultSet result) throws SQLException {
        String definition = result.getString("definition_json");
        JsonNode json = parse(definition);
        return new FeatureViewVersion(
                new FeatureViewId(result.getString("tenant_id"),
                        result.getString("catalog_name"), result.getString("schema_name"),
                        result.getString("feature_view_name")),
                result.getLong("version"), result.getString("owner_principal"),
                textArray(json, "entity_keys"), text(json, "event_time"),
                text(json, "source_table"), text(json, "source_query"),
                result.getBytes("source_plan"), result.getString("source_plan_sha256"),
                text(json, "transform_version"), json.get("schema").toString(),
                json.path("freshness_target_ms").asLong(), json.get("lineage").toString(),
                definition, result.getString("definition_sha256"),
                getInstant(result, "created_at"));
    }

    private static TrainingDatasetVersion mapDataset(ResultSet result) throws SQLException {
        String definition = result.getString("definition_json");
        JsonNode json = parse(definition);
        return new TrainingDatasetVersion(
                new TrainingDatasetId(result.getString("tenant_id"),
                        result.getString("catalog_name"), result.getString("schema_name"),
                        result.getString("dataset_name")),
                result.getLong("version"), result.getString("owner_principal"),
                textMap(json, "source_snapshots"), longMap(json, "feature_versions"),
                text(json, "label"), result.getBytes("normalized_plan"),
                result.getString("normalized_plan_sha256"),
                FeatureSplitKind.valueOf(result.getString("split_kind")), result.getLong("split_seed"),
                json.get("split").toString(), json.get("schema").toString(),
                json.get("lineage").toString(), definition,
                result.getString("definition_sha256"), getInstant(result, "created_at"));
    }

    private static JsonNode parse(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Persisted catalog definition is invalid JSON", failure);
        }
    }

    private static String text(JsonNode node, String field) {
        return node.path(field).asText();
    }

    private static List<String> textArray(JsonNode node, String field) {
        return java.util.stream.StreamSupport.stream(node.path(field).spliterator(), false)
                .map(JsonNode::asText).toList();
    }

    private static Map<String, String> textMap(JsonNode node, String field) {
        Map<String, String> values = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.path(field).fields();
        fields.forEachRemaining(entry -> values.put(entry.getKey(), entry.getValue().asText()));
        return Map.copyOf(values);
    }

    private static Map<String, Long> longMap(JsonNode node, String field) {
        Map<String, Long> values = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.path(field).fields();
        fields.forEachRemaining(entry -> values.put(entry.getKey(), entry.getValue().asLong()));
        return Map.copyOf(values);
    }

    private static void requireFeatureExists(
            Connection connection, FeatureViewId id, long version) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM feature_view_versions WHERE tenant_id=? AND catalog_name=? "
                        + "AND schema_name=? AND feature_view_name=? AND version=? FOR SHARE")) {
            bindFeatureId(statement, id, 1);
            statement.setLong(5, version);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Feature View version does not exist");
                }
            }
        }
    }

    private static OptionalLong insertPublication(
            Connection connection, FeatureViewId id, long version,
            String principal, Instant at) throws SQLException {
        String sql = "INSERT INTO feature_view_active_versions (tenant_id,catalog_name,"
                + "schema_name,feature_view_name,active_version,publication_epoch,published_by,"
                + "published_at) VALUES (?,?,?,?,?,1,?,?) ON CONFLICT DO NOTHING "
                + "RETURNING publication_epoch";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindFeatureId(statement, id, 1);
            statement.setLong(5, version);
            statement.setString(6, principal);
            setInstant(statement, 7, at);
            return returnedLong(statement);
        }
    }

    private static OptionalLong updatePublication(
            Connection connection, FeatureViewId id, long version, long expectedEpoch,
            String principal, Instant at) throws SQLException {
        requireVersion(expectedEpoch);
        String sql = "UPDATE feature_view_active_versions SET active_version=?,"
                + "publication_epoch=publication_epoch+1,published_by=?,published_at=? "
                + "WHERE tenant_id=? AND catalog_name=? AND schema_name=? "
                + "AND feature_view_name=? AND publication_epoch=? RETURNING publication_epoch";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, version);
            statement.setString(2, principal);
            setInstant(statement, 3, at);
            statement.setString(4, id.tenantId());
            statement.setString(5, id.catalogName());
            statement.setString(6, id.schemaName());
            statement.setString(7, id.viewName());
            statement.setLong(8, expectedEpoch);
            return returnedLong(statement);
        }
    }

    private static OptionalLong returnedLong(PreparedStatement statement) throws SQLException {
        try (ResultSet result = statement.executeQuery()) {
            return result.next() ? OptionalLong.of(result.getLong(1)) : OptionalLong.empty();
        }
    }

    private static void bindFeatureId(
            PreparedStatement statement, FeatureViewId id, int index) throws SQLException {
        statement.setString(index, id.tenantId());
        statement.setString(index + 1, id.catalogName());
        statement.setString(index + 2, id.schemaName());
        statement.setString(index + 3, id.viewName());
    }

    private static void bindDatasetId(
            PreparedStatement statement, TrainingDatasetId id, int index) throws SQLException {
        statement.setString(index, id.tenantId());
        statement.setString(index + 1, id.catalogName());
        statement.setString(index + 2, id.schemaName());
        statement.setString(index + 3, id.datasetName());
    }

    private static CatalogExportRequest mapExport(ResultSet result) throws SQLException {
        return new CatalogExportRequest(
                result.getString("tenant_id"),
                CatalogExportKind.valueOf(result.getString("object_kind")),
                result.getString("catalog_name"), result.getString("schema_name"),
                result.getString("object_name"), result.getLong("object_version"),
                result.getString("generation"), result.getString("artifact_id"),
                result.getString("job_id"), result.getString("dependency_sha256"),
                result.getString("requested_by"), getInstant(result, "requested_at"));
    }

    private static void requireVersion(long version) {
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
    }
}
