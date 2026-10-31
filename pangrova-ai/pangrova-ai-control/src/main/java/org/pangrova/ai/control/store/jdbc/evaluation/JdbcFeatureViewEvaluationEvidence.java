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
package org.pangrova.ai.control.store.jdbc.evaluation;

import org.pangrova.ai.control.evaluation.EvaluationMetric;
import org.pangrova.ai.control.evaluation.EvaluationRun;
import org.pangrova.ai.control.evaluation.EvaluationSubjectType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Reads only atomically published Feature View output metadata for evaluation. */
final class JdbcFeatureViewEvaluationEvidence {
    static final String ROW_COUNT_METRIC = "materialized_rows";
    private static final int DOUBLE_SIGNIFICAND_BITS = 53;
    private static final long MAX_EXACT_DOUBLE_INTEGER = 1L << DOUBLE_SIGNIFICAND_BITS;

    private JdbcFeatureViewEvaluationEvidence() {}

    static PublishedEvidence load(Connection connection, EvaluationRun run) throws SQLException {
        if (run.subjectType() != EvaluationSubjectType.FEATURE_VIEW) {
            throw new IllegalArgumentException("evaluation subject must be FEATURE_VIEW");
        }
        String sql = "SELECT g.source_snapshot,g.manifest_uri,g.manifest_sha256,g.row_count,"
                + "d.source_snapshot AS dataset_snapshot,d.manifest_uri AS dataset_manifest_uri,"
                + "d.manifest_sha256 AS dataset_manifest_sha256,d.item_count "
                + "FROM tabular_export_generations g "
                + "JOIN catalog_export_requests c ON c.tenant_id=g.tenant_id "
                + "AND c.job_id=g.job_id AND c.artifact_id=g.artifact_id "
                + "AND c.generation=g.generation AND c.object_kind='FEATURE_VIEW' "
                + "JOIN feature_view_versions f ON f.tenant_id=c.tenant_id "
                + "AND f.catalog_name=c.catalog_name AND f.schema_name=c.schema_name "
                + "AND f.feature_view_name=c.object_name AND f.version=c.object_version "
                + "JOIN artifact_build_jobs b ON b.tenant_id=g.tenant_id AND b.job_id=g.job_id "
                + "AND b.artifact_id=g.artifact_id AND b.staging_generation=g.generation "
                + "AND b.source_snapshot=g.source_snapshot AND b.state='PUBLISHED' "
                + "AND b.manifest_sha256=g.manifest_sha256 "
                + "AND b.published_row_count=g.row_count "
                + "JOIN evaluation_dataset_versions d ON d.tenant_id=g.tenant_id "
                + "AND d.dataset_id=? AND d.version=? "
                + "WHERE g.tenant_id=? AND g.artifact_id=? AND g.generation=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, run.datasetId());
            statement.setLong(2, run.datasetVersion());
            statement.setString(3, run.tenantId());
            statement.setString(4, run.subjectId());
            statement.setString(5, run.subjectVersion());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException(
                            "Feature View generation has no published export evidence");
                }
                long rows = result.getLong("row_count");
                String manifestUri = result.getString("manifest_uri");
                if (!result.getString("source_snapshot").equals(
                        result.getString("dataset_snapshot"))
                        || !manifestUri.equals(result.getString("dataset_manifest_uri"))
                        || !result.getString("manifest_sha256").equals(
                                result.getString("dataset_manifest_sha256"))
                        || rows != result.getLong("item_count")) {
                    throw new IllegalStateException(
                            "Evaluation dataset does not pin the published Feature View output");
                }
                return new PublishedEvidence(rows, manifestUri);
            }
        }
    }

    static void requirePublishedMetric(Connection connection, EvaluationRun run)
            throws SQLException {
        EvaluationMetric expected = load(connection, run).metric();
        String sql = "SELECT metric_name,metric_value,metric_unit,passed,evidence_ref "
                + "FROM evaluation_metrics WHERE tenant_id=? AND run_id=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, run.tenantId());
            statement.setString(2, run.runId());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()
                        || !expected.name().equals(result.getString("metric_name"))
                        || expected.value() != result.getDouble("metric_value")
                        || !expected.unit().equals(result.getString("metric_unit"))
                        || !result.getBoolean("passed")
                        || !expected.evidenceReference().equals(
                                result.getString("evidence_ref"))
                        || result.next()) {
                    throw new IllegalStateException(
                            "Feature View evaluation metric differs from published export");
                }
            }
        }
    }

    record PublishedEvidence(long rows, String manifestUri) {
        PublishedEvidence {
            if (rows < 0 || rows > MAX_EXACT_DOUBLE_INTEGER) {
                throw new IllegalStateException(
                        "Feature View row count exceeds exact evaluation metric precision");
            }
        }

        EvaluationMetric metric() {
            return new EvaluationMetric(
                    ROW_COUNT_METRIC, (double) rows, "rows", true, manifestUri);
        }
    }
}
