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
package org.pangrova.metadata.evaluation;

import org.pangrova.metadata.exception.MetastoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.metadata.store.JdbcSupport.getInstant;
import static org.pangrova.metadata.store.JdbcSupport.setInstant;

/** PostgreSQL implementation of version-pinned evaluation metadata and release gates. */
public final class JdbcEvaluationStore implements EvaluationStore {

    private final DataSource dataSource;

    public JdbcEvaluationStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
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
            throw new MetastoreException("Failed to create evaluation dataset version", failure);
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
            throw new MetastoreException("Failed to find evaluation dataset version", failure);
        }
    }

    @Override
    public void createRun(EvaluationRun run) {
        Objects.requireNonNull(run, "run must not be null");
        if (run.state() != EvaluationRunState.RUNNING) {
            throw new IllegalArgumentException("new evaluation run must be RUNNING");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                validateBaseline(connection, run);
                insertRun(connection, run);
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to create evaluation run", failure);
        }
    }

    @Override
    public Optional<EvaluationRun> findRun(String tenantId, String runId) {
        EvaluationValidation.requireText(tenantId, "tenantId");
        EvaluationValidation.requireText(runId, "runId");
        try (Connection connection = dataSource.getConnection()) {
            return findRun(connection, tenantId, runId, false);
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to find evaluation run", failure);
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
        EvaluationValidation.requireText(tenantId, "tenantId");
        EvaluationValidation.requireText(runId, "runId");
        Objects.requireNonNull(outcome, "outcome must not be null");
        if (!outcome.isTerminal()) {
            throw new IllegalArgumentException("outcome must be terminal");
        }
        Objects.requireNonNull(metrics, "metrics must not be null");
        List<EvaluationMetric> immutableMetrics = List.copyOf(metrics);
        validateUniqueMetrics(immutableMetrics);
        EvaluationValidation.requireOptionalText(traceReference, "traceReference");
        validateFailure(outcome, failureClass);
        Objects.requireNonNull(completedAt, "completedAt must not be null");
        if (outcome == EvaluationRunState.SUCCEEDED && immutableMetrics.isEmpty()) {
            throw new IllegalArgumentException(
                    "successful evaluation requires at least one metric");
        }

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                int changed = updateTerminalRun(
                        connection, tenantId, runId, outcome,
                        traceReference, failureClass, completedAt);
                if (changed == 0) {
                    connection.rollback();
                    return false;
                }
                insertMetrics(connection, tenantId, runId, immutableMetrics);
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to complete evaluation run", failure);
        }
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
            throw new MetastoreException("Failed to list evaluation metrics", failure);
        }
    }

    @Override
    public void recordReleaseDecision(EvaluationReleaseDecision decision) {
        Objects.requireNonNull(decision, "decision must not be null");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                EvaluationRun run = findRun(
                        connection, decision.tenantId(), decision.runId(), true)
                        .orElseThrow(() -> new IllegalStateException(
                                "Evaluation run does not exist: " + decision.runId()));
                validateReleaseDecision(connection, run, decision);
                insertReleaseDecision(connection, decision);
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to record evaluation release decision", failure);
        }
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
            throw new MetastoreException("Failed to find evaluation release decision", failure);
        }
    }

    private static void insertRun(Connection connection, EvaluationRun run) throws SQLException {
        String sql = "INSERT INTO evaluation_runs (tenant_id,run_id,dataset_id,dataset_version,"
                + "subject_type,subject_id,subject_version,baseline_run_id,policy_version,state,"
                + "started_at,completed_at,trace_reference,failure_class) "
                + "VALUES (?,?,?,?,?,?,?,?,?,'RUNNING',?,NULL,NULL,NULL)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, run.tenantId());
            statement.setString(2, run.runId());
            statement.setString(3, run.datasetId());
            statement.setLong(4, run.datasetVersion());
            statement.setString(5, run.subjectType().name());
            statement.setString(6, run.subjectId());
            statement.setString(7, run.subjectVersion());
            statement.setString(8, run.baselineRunId());
            statement.setString(9, run.policyVersion());
            setInstant(statement, 10, run.startedAt());
            statement.executeUpdate();
        }
    }

    private static int updateTerminalRun(
            Connection connection,
            String tenantId,
            String runId,
            EvaluationRunState outcome,
            String traceReference,
            String failureClass,
            Instant completedAt) throws SQLException {
        String sql = "UPDATE evaluation_runs SET state=?,completed_at=?,trace_reference=?,"
                + "failure_class=? WHERE tenant_id=? AND run_id=? AND state='RUNNING'";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, outcome.name());
            setInstant(statement, 2, completedAt);
            statement.setString(3, traceReference);
            statement.setString(4, failureClass);
            statement.setString(5, tenantId);
            statement.setString(6, runId);
            return statement.executeUpdate();
        }
    }

    private static void insertMetrics(
            Connection connection,
            String tenantId,
            String runId,
            List<EvaluationMetric> metrics) throws SQLException {
        if (metrics.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO evaluation_metrics (tenant_id,run_id,metric_name,"
                + "metric_value,metric_unit,passed,evidence_ref) VALUES (?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (EvaluationMetric metric : metrics) {
                statement.setString(1, tenantId);
                statement.setString(2, runId);
                statement.setString(3, metric.name());
                statement.setDouble(4, metric.value());
                statement.setString(5, metric.unit());
                statement.setBoolean(6, metric.passed());
                statement.setString(7, metric.evidenceReference());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private static void validateBaseline(Connection connection, EvaluationRun run)
            throws SQLException {
        if (run.baselineRunId() == null) {
            return;
        }
        EvaluationRun baseline = findRun(
                connection, run.tenantId(), run.baselineRunId(), true)
                .orElseThrow(() -> new IllegalStateException(
                        "Baseline evaluation run does not exist: " + run.baselineRunId()));
        if (baseline.state() != EvaluationRunState.SUCCEEDED) {
            throw new IllegalStateException("Baseline evaluation run must be SUCCEEDED");
        }
        if (baseline.subjectType() != run.subjectType()
                || !baseline.subjectId().equals(run.subjectId())) {
            throw new IllegalStateException(
                    "Baseline evaluation run must evaluate the same subject identity");
        }
        if (!baseline.datasetId().equals(run.datasetId())
                || baseline.datasetVersion() != run.datasetVersion()) {
            throw new IllegalStateException(
                    "Baseline evaluation run must use the same dataset version");
        }
    }

    private static void validateReleaseDecision(
            Connection connection,
            EvaluationRun run,
            EvaluationReleaseDecision decision) throws SQLException {
        if (!run.state().isTerminal()) {
            throw new IllegalStateException("Release decision requires a terminal evaluation run");
        }
        if (!run.policyVersion().equals(decision.policyVersion())) {
            throw new IllegalStateException(
                    "Release decision policy version does not match the evaluation run");
        }
        if (!decision.approved()) {
            return;
        }
        if (run.state() != EvaluationRunState.SUCCEEDED) {
            throw new IllegalStateException("Only a successful evaluation can be approved");
        }
        MetricGate gate = loadMetricGate(connection, run.tenantId(), run.runId());
        if (gate.metricCount() == 0 || gate.failedCount() != 0) {
            throw new IllegalStateException(
                    "Approval requires at least one metric and no failed metrics");
        }
        if (run.baselineRunId() != null) {
            EvaluationRun baseline = findRun(
                    connection, run.tenantId(), run.baselineRunId(), true)
                    .orElseThrow(() -> new IllegalStateException(
                            "Pinned baseline evaluation run no longer exists"));
            if (baseline.state() != EvaluationRunState.SUCCEEDED) {
                throw new IllegalStateException(
                        "Pinned baseline evaluation run must be SUCCEEDED");
            }
            JdbcEvaluationReleaseGate.requireComparableMetrics(
                    connection, run.tenantId(), run.runId(), run.baselineRunId());
        }
    }

    private static MetricGate loadMetricGate(
            Connection connection, String tenantId, String runId) throws SQLException {
        String sql = "SELECT COUNT(*) AS metric_count,"
                + "COUNT(*) FILTER (WHERE NOT passed) AS failed_count "
                + "FROM evaluation_metrics WHERE tenant_id=? AND run_id=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, runId);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return new MetricGate(
                        result.getLong("metric_count"), result.getLong("failed_count"));
            }
        }
    }

    private static void insertReleaseDecision(
            Connection connection, EvaluationReleaseDecision decision) throws SQLException {
        String sql = "INSERT INTO evaluation_release_decisions "
                + "(tenant_id,run_id,approved,policy_version,reason,decided_by,decided_at) "
                + "VALUES (?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, decision.tenantId());
            statement.setString(2, decision.runId());
            statement.setBoolean(3, decision.approved());
            statement.setString(4, decision.policyVersion());
            statement.setString(5, decision.reason());
            statement.setString(6, decision.decidedBy());
            setInstant(statement, 7, decision.decidedAt());
            statement.executeUpdate();
        }
    }

    private static Optional<EvaluationRun> findRun(
            Connection connection, String tenantId, String runId, boolean lock)
            throws SQLException {
        String sql = "SELECT * FROM evaluation_runs WHERE tenant_id=? AND run_id=?"
                + (lock ? " FOR SHARE" : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, runId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapRun(result)) : Optional.empty();
            }
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

    private static EvaluationRun mapRun(ResultSet result) throws SQLException {
        Instant completedAt = getInstant(result, "completed_at");
        return new EvaluationRun(
                result.getString("tenant_id"),
                result.getString("run_id"),
                result.getString("dataset_id"),
                result.getLong("dataset_version"),
                EvaluationSubjectType.valueOf(result.getString("subject_type")),
                result.getString("subject_id"),
                result.getString("subject_version"),
                result.getString("baseline_run_id"),
                result.getString("policy_version"),
                EvaluationRunState.valueOf(result.getString("state")),
                getInstant(result, "started_at"),
                completedAt,
                result.getString("trace_reference"),
                result.getString("failure_class"));
    }

    private static void validateUniqueMetrics(List<EvaluationMetric> metrics) {
        Set<String> names = new HashSet<>();
        for (EvaluationMetric metric : metrics) {
            Objects.requireNonNull(metric, "metric must not be null");
            if (!names.add(metric.name())) {
                throw new IllegalArgumentException(
                        "duplicate evaluation metric: " + metric.name());
            }
        }
    }

    private static void validateFailure(
            EvaluationRunState outcome, String failureClass) {
        EvaluationValidation.requireOptionalText(failureClass, "failureClass");
        if (outcome == EvaluationRunState.SUCCEEDED && failureClass != null) {
            throw new IllegalArgumentException(
                    "successful evaluation must not have a failureClass");
        }
        if (outcome == EvaluationRunState.FAILED && failureClass == null) {
            throw new IllegalArgumentException("failed evaluation requires failureClass");
        }
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

    private record MetricGate(long metricCount, long failedCount) {}
}
