package org.pangrova.persistence.ai.postgresql.evaluation;

import org.pangrova.ai.control.evaluation.EvaluationMetricDirection;
import org.pangrova.ai.control.evaluation.EvaluationMetricPolicy;
import org.pangrova.ai.control.evaluation.EvaluationMetricRule;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Database-local validation for release evidence pinned to a baseline run. */
final class JdbcEvaluationReleaseGate {

    private JdbcEvaluationReleaseGate() {}

    static boolean hasPinnedPolicy(Connection connection, String tenantId, String runId)
            throws SQLException {
        String sql = "SELECT metric_policy_pinned FROM evaluation_runs "
                + "WHERE tenant_id=? AND run_id=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, runId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean("metric_policy_pinned");
            }
        }
    }

    static void requireComparableMetrics(
            Connection connection,
            String tenantId,
            String candidateRunId,
            String baselineRunId) throws SQLException {
        Map<String, MetricEvidence> candidate = loadMetrics(
                connection, tenantId, candidateRunId);
        Map<String, MetricEvidence> baseline = loadMetrics(
                connection, tenantId, baselineRunId);
        if (!candidate.keySet().containsAll(baseline.keySet())) {
            throw new IllegalStateException(
                    "Candidate evaluation is missing metrics from its pinned baseline");
        }
        for (Map.Entry<String, MetricEvidence> entry : baseline.entrySet()) {
            MetricEvidence candidateMetric = candidate.get(entry.getKey());
            if (!candidateMetric.unit().equals(entry.getValue().unit())) {
                throw new IllegalStateException(
                        "Candidate and baseline metric units differ: " + entry.getKey());
            }
            if (!entry.getValue().passed()) {
                throw new IllegalStateException(
                        "Pinned baseline contains a failed metric: " + entry.getKey());
            }
        }
    }

    static Optional<EvaluationMetricPolicy> loadPolicy(
            Connection connection, String tenantId, String version) throws SQLException {
        String headerSql = "SELECT 1 FROM evaluation_metric_policies "
                + "WHERE tenant_id=? AND policy_version=? AND sealed=TRUE";
        try (PreparedStatement statement = connection.prepareStatement(headerSql)) {
            statement.setString(1, tenantId);
            statement.setString(2, version);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
            }
        }
        String rulesSql = "SELECT metric_name,metric_unit,direction,threshold_value,"
                + "maximum_baseline_regression FROM evaluation_metric_policy_rules "
                + "WHERE tenant_id=? AND policy_version=? ORDER BY metric_name";
        try (PreparedStatement statement = connection.prepareStatement(rulesSql)) {
            statement.setString(1, tenantId);
            statement.setString(2, version);
            try (ResultSet result = statement.executeQuery()) {
                List<EvaluationMetricRule> rules = new ArrayList<>();
                while (result.next()) {
                    rules.add(new EvaluationMetricRule(
                            result.getString("metric_name"),
                            result.getString("metric_unit"),
                            EvaluationMetricDirection.valueOf(result.getString("direction")),
                            result.getDouble("threshold_value"),
                            result.getDouble("maximum_baseline_regression")));
                }
                return Optional.of(new EvaluationMetricPolicy(tenantId, version, rules));
            }
        }
    }

    static void requireNumericalPolicy(
            Connection connection, String tenantId, String candidateRunId,
            String baselineRunId, String policyVersion) throws SQLException {
        EvaluationMetricPolicy policy = loadPolicy(connection, tenantId, policyVersion)
                .orElseThrow(() -> new IllegalStateException(
                        "Evaluation metric policy does not exist: " + policyVersion));
        Map<String, MetricEvidence> candidate = loadMetrics(connection, tenantId, candidateRunId);
        Map<String, MetricEvidence> baseline = baselineRunId == null
                ? Map.of() : loadMetrics(connection, tenantId, baselineRunId);
        if (candidate.size() != policy.rules().size()) {
            throw new IllegalStateException(
                    "Candidate evaluation metrics do not match required metric policy rules");
        }
        for (EvaluationMetricRule rule : policy.rules()) {
            MetricEvidence metric = candidate.get(rule.name());
            if (metric == null) {
                throw new IllegalStateException(
                        "Candidate evaluation is missing required metric: " + rule.name());
            }
            if (!rule.unit().equals(metric.unit())) {
                throw new IllegalStateException(
                        "Candidate metric unit differs from policy: " + rule.name());
            }
            boolean meetsThreshold = switch (rule.direction()) {
                case HIGHER_IS_BETTER -> metric.value() >= rule.threshold();
                case LOWER_IS_BETTER -> metric.value() <= rule.threshold();
            };
            if (!meetsThreshold) {
                throw new IllegalStateException(
                        "Candidate metric violates numerical threshold: " + rule.name());
            }
            MetricEvidence baselineMetric = baseline.get(rule.name());
            if (baselineMetric == null) {
                continue;
            }
            // Compare only values with the same explicit unit.  The earlier
            // comparable-metrics check also rejects missing/failed baseline evidence.
            if (!rule.unit().equals(baselineMetric.unit())) {
                throw new IllegalStateException(
                        "Pinned baseline metric unit differs from policy: " + rule.name());
            }
            double regression = switch (rule.direction()) {
                case HIGHER_IS_BETTER -> baselineMetric.value() - metric.value();
                case LOWER_IS_BETTER -> metric.value() - baselineMetric.value();
            };
            if (regression > rule.maximumBaselineRegression()) {
                throw new IllegalStateException(
                        "Candidate metric regresses from pinned baseline: " + rule.name());
            }
        }
    }

    private static Map<String, MetricEvidence> loadMetrics(
            Connection connection, String tenantId, String runId) throws SQLException {
        String sql = "SELECT metric_name,metric_value,metric_unit,passed FROM evaluation_metrics "
                + "WHERE tenant_id=? AND run_id=? ORDER BY metric_name";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, runId);
            try (ResultSet result = statement.executeQuery()) {
                Map<String, MetricEvidence> metrics = new LinkedHashMap<>();
                while (result.next()) {
                    metrics.put(result.getString("metric_name"), new MetricEvidence(
                            result.getDouble("metric_value"),
                            result.getString("metric_unit"), result.getBoolean("passed")));
                }
                return metrics;
            }
        }
    }

    private record MetricEvidence(double value, String unit, boolean passed) {}
}
