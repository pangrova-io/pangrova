package org.pangrova.metadata.evaluation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Database-local validation for release evidence pinned to a baseline run. */
final class JdbcEvaluationReleaseGate {

    private JdbcEvaluationReleaseGate() {}

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

    private static Map<String, MetricEvidence> loadMetrics(
            Connection connection, String tenantId, String runId) throws SQLException {
        String sql = "SELECT metric_name,metric_unit,passed FROM evaluation_metrics "
                + "WHERE tenant_id=? AND run_id=? ORDER BY metric_name";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, runId);
            try (ResultSet result = statement.executeQuery()) {
                Map<String, MetricEvidence> metrics = new LinkedHashMap<>();
                while (result.next()) {
                    metrics.put(result.getString("metric_name"), new MetricEvidence(
                            result.getString("metric_unit"), result.getBoolean("passed")));
                }
                return metrics;
            }
        }
    }

    private record MetricEvidence(String unit, boolean passed) {}
}
