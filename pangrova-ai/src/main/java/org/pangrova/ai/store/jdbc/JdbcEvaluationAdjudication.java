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

import org.pangrova.ai.api.EvaluationMetric;
import org.pangrova.ai.api.EvaluationReleaseDecision;
import org.pangrova.ai.api.EvaluationRun;
import org.pangrova.ai.api.EvaluationRunState;
import org.pangrova.ai.api.EvaluationSubjectType;
import org.pangrova.ai.api.EvaluationValidation;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.ai.store.jdbc.JdbcAiSupport.getInstant;
import static org.pangrova.ai.store.jdbc.JdbcAiSupport.setInstant;

/**
 * Run lifecycle transactions and release-gate adjudication of the JDBC
 * evaluation authority.
 *
 * <p>Every mutating method owns one transaction and validates the evidence
 * pinned to the run: baseline identity, terminal transitions, published
 * Feature View metrics, and the sealed numerical policy. The evaluation
 * facade delegates these transitions; the collaborator never performs
 * data-plane I/O.</p>
 */
final class JdbcEvaluationAdjudication {

    private final DataSource dataSource;

    JdbcEvaluationAdjudication(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    void createRun(EvaluationRun run) {
        Objects.requireNonNull(run, "run must not be null");
        if (run.state() != EvaluationRunState.RUNNING) {
            throw new IllegalArgumentException("new evaluation run must be RUNNING");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (JdbcEvaluationReleaseGate.loadPolicy(
                        connection, run.tenantId(), run.policyVersion()).isEmpty()) {
                    throw new IllegalStateException(
                            "Evaluation metric policy does not exist: " + run.policyVersion());
                }
                validateBaseline(connection, run);
                insertRun(connection, run);
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new AiStoreException("Failed to create evaluation run", failure);
        }
    }

    boolean completeRun(
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
            throw new AiStoreException("Failed to complete evaluation run", failure);
        }
    }

    boolean completeFeatureViewMaterializationRun(
            String tenantId, String runId, Instant completedAt) {
        EvaluationValidation.requireText(tenantId, "tenantId");
        EvaluationValidation.requireText(runId, "runId");
        Objects.requireNonNull(completedAt, "completedAt must not be null");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                EvaluationRun run = findRun(connection, tenantId, runId, false)
                        .orElseThrow(() -> new IllegalStateException(
                                "Evaluation run does not exist: " + runId));
                if (run.state() != EvaluationRunState.RUNNING) {
                    connection.rollback();
                    return false;
                }
                EvaluationMetric metric = JdbcFeatureViewEvaluationEvidence
                        .load(connection, run).metric();
                int changed = updateTerminalRun(
                        connection, tenantId, runId, EvaluationRunState.SUCCEEDED,
                        metric.evidenceReference(), null, completedAt);
                if (changed == 0) {
                    connection.rollback();
                    return false;
                }
                insertMetrics(connection, tenantId, runId, List.of(metric));
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new AiStoreException(
                    "Failed to complete Feature View materialization evaluation", failure);
        }
    }

    void recordReleaseDecision(EvaluationReleaseDecision decision) {
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
            throw new AiStoreException("Failed to record evaluation release decision", failure);
        }
    }

    /**
     * Loads an evaluation run inside a caller-owned transaction.
     *
     * @param connection live connection of the enclosing transaction
     * @param tenantId owning tenant
     * @param runId run identity
     * @param lock whether to acquire a shared lock on the run row
     * @return run when present
     */
    static Optional<EvaluationRun> findRun(
            Connection connection, String tenantId, String runId, boolean lock)
            throws SQLException {
        if (lock) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT * FROM evaluation_runs WHERE tenant_id=? AND run_id=? FOR SHARE")) {
                return executeRunQuery(statement, tenantId, runId);
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM evaluation_runs WHERE tenant_id=? AND run_id=?")) {
            return executeRunQuery(statement, tenantId, runId);
        }
    }

    private static Optional<EvaluationRun> executeRunQuery(
            PreparedStatement statement, String tenantId, String runId) throws SQLException {
        statement.setString(1, tenantId);
        statement.setString(2, runId);
        try (ResultSet result = statement.executeQuery()) {
            return result.next() ? Optional.of(mapRun(result)) : Optional.empty();
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
        String startSql = "SELECT started_at FROM evaluation_runs "
                + "WHERE tenant_id=? AND run_id=? AND state='RUNNING'";
        try (PreparedStatement start = connection.prepareStatement(startSql)) {
            start.setString(1, tenantId);
            start.setString(2, runId);
            try (ResultSet result = start.executeQuery()) {
                if (result.next()
                        && completedAt.isBefore(getInstant(result, "started_at"))) {
                    throw new IllegalArgumentException(
                            "completedAt must not precede startedAt");
                }
            }
        }
        String sql = "UPDATE evaluation_runs SET state=?,completed_at=?,trace_reference=?,"
                + "failure_class=? WHERE tenant_id=? AND run_id=? AND state='RUNNING' RETURNING started_at";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, outcome.name());
            setInstant(statement, 2, completedAt);
            statement.setString(3, traceReference);
            statement.setString(4, failureClass);
            statement.setString(5, tenantId);
            statement.setString(6, runId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return 0;
                }
                if (completedAt.isBefore(getInstant(result, "started_at"))) {
                    throw new IllegalArgumentException("completedAt must not precede startedAt");
                }
                return 1;
            }
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
        if (decision.decidedAt().isBefore(run.completedAt())) {
            throw new IllegalArgumentException("decidedAt must not precede evaluation completedAt");
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
        if (run.subjectType() == EvaluationSubjectType.FEATURE_VIEW) {
            JdbcFeatureViewEvaluationEvidence.requirePublishedMetric(connection, run);
        }
        if (!JdbcEvaluationReleaseGate.hasPinnedPolicy(
                connection, run.tenantId(), run.runId())) {
            throw new IllegalStateException(
                    "Approval requires a metric policy pinned before evaluation run creation");
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
        JdbcEvaluationReleaseGate.requireNumericalPolicy(
                connection, run.tenantId(), run.runId(), run.baselineRunId(),
                run.policyVersion());
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

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private record MetricGate(long metricCount, long failedCount) {}
}
