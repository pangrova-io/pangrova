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
package org.pangrova.ai.store.jdbc;

import org.pangrova.ai.api.EvaluationDatasetId;
import org.pangrova.ai.api.EvaluationDatasetVersion;
import org.pangrova.ai.api.EvaluationMetric;
import org.pangrova.ai.api.EvaluationMetricPolicy;
import org.pangrova.ai.api.EvaluationMetricRule;
import org.pangrova.ai.api.EvaluationReleaseDecision;
import org.pangrova.ai.api.EvaluationRun;
import org.pangrova.ai.api.EvaluationRunState;
import org.pangrova.ai.api.EvaluationStore;
import org.pangrova.ai.api.EvaluationValidation;

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

import static org.pangrova.ai.store.jdbc.JdbcAiSupport.getInstant;
import static org.pangrova.ai.store.jdbc.JdbcAiSupport.setInstant;

/**
 * PostgreSQL implementation of version-pinned evaluation metadata and release gates.
 *
 * <p>Policy, dataset, and read paths live here; run lifecycle transitions and
 * release-gate adjudication are delegated to {@link JdbcEvaluationAdjudication}
 * and the database-local {@link JdbcEvaluationReleaseGate} checks.</p>
 */
public final class JdbcEvaluationStore implements EvaluationStore {

    private final DataSource dataSource;
    private final JdbcEvaluationAdjudication adjudication;

    public JdbcEvaluationStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.adjudication = new JdbcEvaluationAdjudication(dataSource);
    }

    @Override
    public void createMetricPolicy(EvaluationMetricPolicy policy) {
        Objects.requireNonNull(policy, "policy must not be null");
        String parentSql = "INSERT INTO evaluation_metric_policies (tenant_id,policy_version) VALUES (?,?)";
        String ruleSql = "INSERT INTO evaluation_metric_policy_rules "
                + "(tenant_id,policy_version,metric_name,metric_unit,direction,"
                + "threshold_value,maximum_baseline_regression) VALUES (?,?,?,?,?,?,?)";
        String sealSql = "UPDATE evaluation_metric_policies SET sealed=TRUE "
                + "WHERE tenant_id=? AND policy_version=?";
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement parent = connection.prepareStatement(parentSql);
                    PreparedStatement rule = connection.prepareStatement(ruleSql)) {
                parent.setString(1, policy.tenantId());
                parent.setString(2, policy.version());
                parent.executeUpdate();
                for (EvaluationMetricRule metric : policy.rules()) {
                    rule.setString(1, policy.tenantId());
                    rule.setString(2, policy.version());
                    rule.setString(3, metric.name());
                    rule.setString(4, metric.unit());
                    rule.setString(5, metric.direction().name());
                    rule.setDouble(6, metric.threshold());
                    rule.setDouble(7, metric.maximumBaselineRegression());
                    rule.addBatch();
                }
                rule.executeBatch();
                try (PreparedStatement seal = connection.prepareStatement(sealSql)) {
                    seal.setString(1, policy.tenantId());
                    seal.setString(2, policy.version());
                    seal.executeUpdate();
                }
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new AiStoreException("Failed to create evaluation metric policy", failure);
        }
    }

    @Override
    public Optional<EvaluationMetricPolicy> findMetricPolicy(String tenantId, String version) {
        EvaluationValidation.requireText(tenantId, "tenantId");
        EvaluationValidation.requireText(version, "version");
        try (Connection connection = dataSource.getConnection()) {
            return JdbcEvaluationReleaseGate.loadPolicy(connection, tenantId, version);
        } catch (SQLException failure) {
            throw new AiStoreException("Failed to find evaluation metric policy", failure);
        }
    }

    @Override
    public void createDatasetVersion(EvaluationDatasetVersion dataset) {
        Objects.requireNonNull(dataset, "dataset must not be null");
        String sql = "INSERT INTO evaluation_dataset_versions (tenant_id,dataset_id,version,"
                + "owner_principal,source_snapshot,manifest_uri,manifest_sha256,item_count,"
                + "schema_json,created_at) VALUES (?,?,?,?,?,?,?,?,CAST(? AS jsonb),?)";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, dataset.id().tenantId());
            statement.setString(2, dataset.id().datasetId());
            statement.setLong(3, dataset.version());
            statement.setString(4, dataset.ownerPrincipal());
            statement.setString(5, dataset.sourceSnapshot());
            statement.setString(6, dataset.manifestUri());
            statement.setString(7, dataset.manifestSha256().toLowerCase(Locale.ROOT));
            statement.setLong(8, dataset.itemCount());
            statement.setString(9, dataset.schemaJson());
            setInstant(statement, 10, dataset.createdAt());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw new AiStoreException("Failed to create evaluation dataset version", failure);
        }
    }

    @Override
    public Optional<EvaluationDatasetVersion> findDatasetVersion(
            EvaluationDatasetId id, long version) {
        Objects.requireNonNull(id, "id must not be null");
        requirePositive(version, "version");
        String sql = "SELECT * FROM evaluation_dataset_versions "
                + "WHERE tenant_id=? AND dataset_id=? AND version=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, id.tenantId());
            statement.setString(2, id.datasetId());
            statement.setLong(3, version);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapDataset(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new AiStoreException("Failed to find evaluation dataset version", failure);
        }
    }

    @Override
    public void createRun(EvaluationRun run) {
        adjudication.createRun(run);
    }

    @Override
    public Optional<EvaluationRun> findRun(String tenantId, String runId) {
        EvaluationValidation.requireText(tenantId, "tenantId");
        EvaluationValidation.requireText(runId, "runId");
        try (Connection connection = dataSource.getConnection()) {
            return JdbcEvaluationAdjudication.findRun(connection, tenantId, runId, false);
        } catch (SQLException failure) {
            throw new AiStoreException("Failed to find evaluation run", failure);
        }
    }

    @Override
    public boolean completeRun(
            String tenantId,
            String runId,
            EvaluationRunState outcome,
            List<EvaluationMetric> metrics,
            String traceReference,
            String failureClass,
            Instant completedAt) {
        return adjudication.completeRun(
                tenantId, runId, outcome, metrics, traceReference, failureClass, completedAt);
    }

    @Override
    public boolean completeFeatureViewMaterializationRun(
            String tenantId, String runId, Instant completedAt) {
        return adjudication.completeFeatureViewMaterializationRun(tenantId, runId, completedAt);
    }

    @Override
    public List<EvaluationMetric> listMetrics(String tenantId, String runId) {
        EvaluationValidation.requireText(tenantId, "tenantId");
        EvaluationValidation.requireText(runId, "runId");
        String sql = "SELECT metric_name,metric_value,metric_unit,passed,evidence_ref "
                + "FROM evaluation_metrics WHERE tenant_id=? AND run_id=? ORDER BY metric_name";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, runId);
            try (ResultSet result = statement.executeQuery()) {
                List<EvaluationMetric> metrics = new ArrayList<>();
                while (result.next()) {
                    metrics.add(new EvaluationMetric(
                            result.getString("metric_name"),
                            result.getDouble("metric_value"),
                            result.getString("metric_unit"),
                            result.getBoolean("passed"),
                            result.getString("evidence_ref")));
                }
                return List.copyOf(metrics);
            }
        } catch (SQLException failure) {
            throw new AiStoreException("Failed to list evaluation metrics", failure);
        }
    }

    @Override
    public void recordReleaseDecision(EvaluationReleaseDecision decision) {
        adjudication.recordReleaseDecision(decision);
    }

    @Override
    public Optional<EvaluationReleaseDecision> findReleaseDecision(
            String tenantId, String runId) {
        EvaluationValidation.requireText(tenantId, "tenantId");
        EvaluationValidation.requireText(runId, "runId");
        String sql = "SELECT * FROM evaluation_release_decisions "
                + "WHERE tenant_id=? AND run_id=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, runId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(new EvaluationReleaseDecision(
                        result.getString("tenant_id"),
                        result.getString("run_id"),
                        result.getBoolean("approved"),
                        result.getString("policy_version"),
                        result.getString("reason"),
                        result.getString("decided_by"),
                        getInstant(result, "decided_at")));
            }
        } catch (SQLException failure) {
            throw new AiStoreException("Failed to find evaluation release decision", failure);
        }
    }

    private static EvaluationDatasetVersion mapDataset(ResultSet result) throws SQLException {
        return new EvaluationDatasetVersion(
                new EvaluationDatasetId(
                        result.getString("tenant_id"), result.getString("dataset_id")),
                result.getLong("version"),
                result.getString("owner_principal"),
                result.getString("source_snapshot"),
                result.getString("manifest_uri"),
                result.getString("manifest_sha256"),
                result.getLong("item_count"),
                result.getString("schema_json"),
                getInstant(result, "created_at"));
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
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
