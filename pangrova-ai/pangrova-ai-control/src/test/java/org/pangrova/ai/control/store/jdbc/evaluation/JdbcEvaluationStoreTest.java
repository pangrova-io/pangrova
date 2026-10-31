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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.control.api.AiControlException;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.pangrova.ai.control.evaluation.EvaluationDatasetId;
import org.pangrova.ai.control.evaluation.EvaluationDatasetVersion;
import org.pangrova.ai.control.evaluation.EvaluationMetric;
import org.pangrova.ai.control.evaluation.EvaluationMetricDirection;
import org.pangrova.ai.control.evaluation.EvaluationMetricPolicy;
import org.pangrova.ai.control.evaluation.EvaluationMetricRule;
import org.pangrova.ai.control.evaluation.EvaluationReleaseDecision;
import org.pangrova.ai.control.evaluation.EvaluationRun;
import org.pangrova.ai.control.evaluation.EvaluationRunState;
import org.pangrova.ai.control.evaluation.EvaluationStore;
import org.pangrova.ai.control.evaluation.EvaluationSubjectType;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcEvaluationStoreTest extends org.pangrova.testing.testkit.ContainerTestBase {

    private static final String TENANT = "tenant-a";
    private static final Instant NOW = Instant.parse("2026-09-19T00:00:00Z");

    private EvaluationStore store;
    private DataSource dataSource;

    @Test
    void featureViewRowCountMetricRejectsIntegersThatDoubleCannotRepresentExactly() {
        assertThat(new JdbcFeatureViewEvaluationEvidence.PublishedEvidence(
                1L << 53, "metadata://manifest/exact").metric().value())
                .isEqualTo((double) (1L << 53));
        assertThatThrownBy(() -> new JdbcFeatureViewEvaluationEvidence.PublishedEvidence(
                (1L << 53) + 1, "metadata://manifest/rounded"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exact evaluation metric precision");
    }

    @Test
    void directSqlCannotAppendOrMutateMetricsOrPublishedDecision() {
        store.createRun(runningRun("candidate", null, "model-v2"));
        assertSqlRejected("UPDATE evaluation_runs SET state='SUCCEEDED',"
                + "completed_at='2026-09-19 00:00:00+00' "
                + "WHERE tenant_id='tenant-a' AND run_id='candidate'",
                "chk_evaluation_run_time_order");
        assertThat(store.findRun(TENANT, "candidate").orElseThrow().state())
                .isEqualTo(EvaluationRunState.RUNNING);
        assertSqlRejected("INSERT INTO evaluation_metrics "
                + "(tenant_id,run_id,metric_name,metric_value,metric_unit,passed) "
                + "VALUES ('tenant-a','candidate','early',0.9,'ratio',TRUE)",
                "completion transaction");
        assertThat(store.completeRun(TENANT, "candidate", EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("quality", 0.9, "ratio", true, null)),
                null, null, NOW.plusSeconds(10))).isTrue();
        assertSqlRejected("INSERT INTO evaluation_metrics "
                + "(tenant_id,run_id,metric_name,metric_value,metric_unit,passed) "
                + "VALUES ('tenant-a','candidate','late',0.9,'ratio',TRUE)",
                "completion transaction");
        assertSqlRejected("UPDATE evaluation_metrics SET metric_value=0 "
                + "WHERE tenant_id='tenant-a' AND run_id='candidate'", "immutable");
        assertSqlRejected("DELETE FROM evaluation_metrics "
                + "WHERE tenant_id='tenant-a' AND run_id='candidate'", "immutable");
        assertSqlRejected("TRUNCATE evaluation_metrics", "immutable");

        assertSqlRejected("INSERT INTO evaluation_release_decisions "
                + "(tenant_id,run_id,approved,policy_version,reason,decided_by,decided_at) "
                + "VALUES ('tenant-a','candidate',TRUE,'quality-policy-v3',"
                + "'early approval','release-owner','2026-09-19 00:00:09+00')",
                "decided_at not before completed_at");

        EvaluationReleaseDecision decision = approval("candidate", "quality-policy-v3");
        store.recordReleaseDecision(decision);
        assertSqlRejected("UPDATE evaluation_release_decisions SET approved=FALSE "
                + "WHERE tenant_id='tenant-a' AND run_id='candidate'", "immutable");
        assertSqlRejected("DELETE FROM evaluation_release_decisions "
                + "WHERE tenant_id='tenant-a' AND run_id='candidate'", "immutable");
        assertSqlRejected("TRUNCATE evaluation_release_decisions", "immutable");
        assertSqlRejected("UPDATE evaluation_dataset_versions SET item_count=0 "
                + "WHERE tenant_id='tenant-a' AND dataset_id='golden-qa'", "immutable");
        assertThat(store.listMetrics(TENANT, "candidate"))
                .containsExactly(new EvaluationMetric("quality", 0.9, "ratio", true, null));
        assertThat(store.findReleaseDecision(TENANT, "candidate")).contains(decision);
    }

    @Test
    void directSqlCannotEraseAnUnpublishedRun() {
        store.createRun(runningRun("unpublished", null, "model-v2"));
        assertSqlRejected("DELETE FROM evaluation_runs WHERE tenant_id='tenant-a' "
                + "AND run_id='unpublished'", "immutable");
        assertSqlRejected("TRUNCATE evaluation_runs CASCADE", "immutable");
        assertThat(store.findRun(TENANT, "unpublished")).isPresent();
    }

    @BeforeEach
    void setUp() throws Exception {
        dataSource = MigratedPostgres.create();
        store = new JdbcEvaluationStore(dataSource);
        store.createDatasetVersion(dataset());
        store.createMetricPolicy(new EvaluationMetricPolicy(TENANT, "quality-policy-v3", List.of(
                new EvaluationMetricRule("quality", "ratio",
                        EvaluationMetricDirection.HIGHER_IS_BETTER, 0.8, 0))));
    }

    @Test
    void pinsDatasetAndBaselineAndPersistsAReleaseGate() {
        store.createMetricPolicy(new EvaluationMetricPolicy(TENANT, "qa-policy-v1", List.of(
                new EvaluationMetricRule("answer_correctness", "ratio",
                        EvaluationMetricDirection.HIGHER_IS_BETTER, 0.8, 0),
                new EvaluationMetricRule("p95_latency", "milliseconds",
                        EvaluationMetricDirection.LOWER_IS_BETTER, 200, 0))));
        store.createRun(runningRun("baseline", null, "model-v1", "qa-policy-v1"));
        assertThat(store.completeRun(
                TENANT,
                "baseline",
                EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric(
                        "answer_correctness", 0.91, "ratio", true, "evidence/baseline")),
                "trace/baseline",
                null,
                NOW.plusSeconds(10)))
                .isTrue();

        store.createRun(runningRun("candidate", "baseline", "model-v2", "qa-policy-v1"));
        assertThat(store.completeRun(
                TENANT,
                "candidate",
                EvaluationRunState.SUCCEEDED,
                List.of(
                        new EvaluationMetric(
                                "answer_correctness", 0.95, "ratio", true, "evidence/quality"),
                        new EvaluationMetric(
                                "p95_latency", 120, "milliseconds", true, "evidence/latency")),
                "trace/candidate",
                null,
                NOW.plusSeconds(20)))
                .isTrue();
        assertThat(store.completeRun(
                TENANT,
                "candidate",
                EvaluationRunState.FAILED,
                List.of(),
                null,
                "late-retry",
                NOW.plusSeconds(21)))
                .isFalse();

        EvaluationReleaseDecision decision = new EvaluationReleaseDecision(
                TENANT,
                "candidate",
                true,
                "qa-policy-v1",
                "candidate passes pinned release policy",
                "release-owner",
                NOW.plusSeconds(30));
        store.recordReleaseDecision(decision);

        assertThat(store.findDatasetVersion(
                new EvaluationDatasetId(TENANT, "golden-qa"), 7).orElseThrow())
                .satisfies(dataset -> {
                    assertThat(dataset.sourceSnapshot()).isEqualTo("iceberg-snapshot-442");
                    assertThat(dataset.schemaJson()).contains("input", "expected");
                });
        assertThat(store.findRun(TENANT, "candidate").orElseThrow())
                .satisfies(run -> {
                    assertThat(run.datasetVersion()).isEqualTo(7);
                    assertThat(run.baselineRunId()).isEqualTo("baseline");
                    assertThat(run.state()).isEqualTo(EvaluationRunState.SUCCEEDED);
                    assertThat(run.traceReference()).isEqualTo("trace/candidate");
                });
        assertThat(store.listMetrics(TENANT, "candidate"))
                .extracting(EvaluationMetric::name)
                .containsExactly("answer_correctness", "p95_latency");
        assertThat(store.findReleaseDecision(TENANT, "candidate")).contains(decision);
        assertThat(store.findRun("tenant-b", "candidate")).isEmpty();
    }

    @Test
    void failedMetricBlocksApprovalButAllowsAnAuditedRejection() {
        store.createRun(runningRun("candidate", null, "model-v2"));
        store.completeRun(
                TENANT,
                "candidate",
                EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric(
                        "safety", 0.60, "ratio", false, "evidence/safety")),
                null,
                null,
                NOW.plusSeconds(10));

        assertThatThrownBy(() -> store.recordReleaseDecision(new EvaluationReleaseDecision(
                TENANT,
                "candidate",
                true,
                "quality-policy-v3",
                "approve despite regression",
                "release-owner",
                NOW.plusSeconds(11))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no failed metrics");

        EvaluationReleaseDecision rejection = new EvaluationReleaseDecision(
                TENANT,
                "candidate",
                false,
                "quality-policy-v3",
                "safety regression",
                "release-owner",
                NOW.plusSeconds(12));
        store.recordReleaseDecision(rejection);
        assertThat(store.findReleaseDecision(TENANT, "candidate")).contains(rejection);
    }

    @Test
    void approvalRequiresBaselineMetricsWithTheSameNamesAndUnits() {
        store.createRun(runningRun("baseline", null, "model-v1"));
        store.completeRun(
                TENANT,
                "baseline",
                EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("quality", 0.90, "ratio", true, null)),
                null,
                null,
                NOW.plusSeconds(5));
        store.createRun(runningRun("candidate", "baseline", "model-v2"));
        store.completeRun(
                TENANT,
                "candidate",
                EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("quality", 95, "percent", true, null)),
                null,
                null,
                NOW.plusSeconds(10));

        assertThatThrownBy(() -> store.recordReleaseDecision(new EvaluationReleaseDecision(
                TENANT,
                "candidate",
                true,
                "quality-policy-v3",
                "incomparable units",
                "release-owner",
                NOW.plusSeconds(11))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("units differ");
        assertThat(store.findReleaseDecision(TENANT, "candidate")).isEmpty();
    }

    @Test
    void approvalRejectsCandidateThatOmitsABaselineMetric() {
        store.createRun(runningRun("baseline", null, "model-v1"));
        store.completeRun(
                TENANT,
                "baseline",
                EvaluationRunState.SUCCEEDED,
                List.of(
                        new EvaluationMetric("quality", 0.90, "ratio", true, null),
                        new EvaluationMetric("safety", 1.0, "ratio", true, null)),
                null,
                null,
                NOW.plusSeconds(5));
        store.createRun(runningRun("candidate", "baseline", "model-v2"));
        store.completeRun(
                TENANT,
                "candidate",
                EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("quality", 0.95, "ratio", true, null)),
                null,
                null,
                NOW.plusSeconds(10));

        assertThatThrownBy(() -> store.recordReleaseDecision(new EvaluationReleaseDecision(
                TENANT,
                "candidate",
                true,
                "quality-policy-v3",
                "missing safety evidence",
                "release-owner",
                NOW.plusSeconds(11))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing metrics");
        assertThat(store.findReleaseDecision(TENANT, "candidate")).isEmpty();
    }

    @Test
    void invalidBaselineAndDuplicateMetricsDoNotPublishPartialRunState() {
        assertThatThrownBy(() -> store.createRun(
                runningRun("candidate", "missing-baseline", "model-v2")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
        assertThat(store.findRun(TENANT, "candidate")).isEmpty();

        store.createRun(runningRun("candidate", null, "model-v2"));
        EvaluationMetric metric = new EvaluationMetric("quality", 0.9, "ratio", true, null);
        assertThatThrownBy(() -> store.completeRun(
                TENANT,
                "candidate",
                EvaluationRunState.SUCCEEDED,
                List.of(metric, metric),
                null,
                null,
                NOW.plusSeconds(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate evaluation metric");
        assertThat(store.findRun(TENANT, "candidate").orElseThrow().state())
                .isEqualTo(EvaluationRunState.RUNNING);
        assertThat(store.listMetrics(TENANT, "candidate")).isEmpty();
    }

    @Test
    void datasetVersionsAndReleaseDecisionsAreImmutable() {
        assertThatThrownBy(() -> store.createDatasetVersion(dataset()))
                .isInstanceOf(AiControlException.class);

        store.createRun(runningRun("candidate", null, "model-v2"));
        store.completeRun(
                TENANT,
                "candidate",
                EvaluationRunState.FAILED,
                List.of(),
                "trace/failed",
                "provider-timeout",
                NOW.plusSeconds(10));
        EvaluationReleaseDecision rejection = new EvaluationReleaseDecision(
                TENANT,
                "candidate",
                false,
                "quality-policy-v3",
                "evaluation failed",
                "release-owner",
                NOW.plusSeconds(11));
        store.recordReleaseDecision(rejection);

        assertThatThrownBy(() -> store.recordReleaseDecision(rejection))
                .isInstanceOf(AiControlException.class);
    }

    @Test
    void completionBeforeStartRollsBackStateAndMetricsForEveryTerminalOutcome() {
        for (EvaluationRunState outcome : List.of(
                EvaluationRunState.SUCCEEDED, EvaluationRunState.FAILED, EvaluationRunState.CANCELLED)) {
            String runId = "early-" + outcome.name();
            store.createRun(runningRun(runId, null, "model-v2"));
            List<EvaluationMetric> metrics = List.of(new EvaluationMetric("quality", 0.9, "ratio", true, null));
            String failureClass = outcome == EvaluationRunState.FAILED ? "evaluation-failed" : null;
            assertThatThrownBy(() -> store.completeRun(
                    TENANT, runId, outcome, metrics, null, failureClass, NOW))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must not precede startedAt");
            assertThat(store.findRun(TENANT, runId).orElseThrow().state())
                    .isEqualTo(EvaluationRunState.RUNNING);
            assertThat(store.listMetrics(TENANT, runId)).isEmpty();
            assertThat(store.completeRun(
                    TENANT, runId, outcome, metrics, null, failureClass, NOW.plusSeconds(1))).isTrue();
            assertThat(store.findRun(TENANT, runId).orElseThrow().completedAt()).isEqualTo(NOW.plusSeconds(1));
            assertThat(store.listMetrics(TENANT, runId)).containsExactlyElementsOf(metrics);
        }
    }

    @Test
    void releaseDecisionCannotPrecedeCompletionAndRejectedAttemptPublishesNothing() {
        for (boolean approved : List.of(true, false)) {
            String runId = "decision-" + approved;
            store.createRun(runningRun(runId, null, "model-v2"));
            assertThat(store.completeRun(TENANT, runId, EvaluationRunState.SUCCEEDED,
                    List.of(new EvaluationMetric("quality", 0.9, "ratio", true, null)),
                    null, null, NOW.plusSeconds(10))).isTrue();
            assertThatThrownBy(() -> store.recordReleaseDecision(new EvaluationReleaseDecision(
                    TENANT, runId, approved, "quality-policy-v3", "evaluation outcome", "release-owner",
                    NOW.plusSeconds(9))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must not precede evaluation completedAt");
            assertThat(store.findReleaseDecision(TENANT, runId)).isEmpty();
            EvaluationReleaseDecision valid = new EvaluationReleaseDecision(
                    TENANT, runId, approved, "quality-policy-v3", "evaluation outcome", "release-owner",
                    NOW.plusSeconds(10));
            store.recordReleaseDecision(valid);
            assertThat(store.findReleaseDecision(TENANT, runId)).contains(valid);
        }
    }

    @Test
    void actualPostgresAccuracyRegressionBlocksReleaseDespitePassingCallerFlag() throws Exception {
        double baselineAccuracy = accuracyFromActualObservations(9);
        double candidateAccuracy = accuracyFromActualObservations(8);
        store.createRun(runningRun("baseline", null, "model-v1"));
        store.completeRun(TENANT, "baseline", EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("quality", baselineAccuracy, "ratio", true,
                        "postgres/observations/baseline")),
                null, null, NOW.plusSeconds(5));
        store.createRun(runningRun("candidate", "baseline", "model-v2"));
        store.completeRun(TENANT, "candidate", EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("quality", candidateAccuracy, "ratio", true,
                        "postgres/observations/candidate")),
                null, null, NOW.plusSeconds(10));

        assertThat(baselineAccuracy).isEqualTo(0.9);
        assertThat(candidateAccuracy).isEqualTo(0.8);
        assertThat(store.listMetrics(TENANT, "candidate").get(0).value())
                .isEqualTo(candidateAccuracy);
        assertThatThrownBy(() -> store.recordReleaseDecision(approval("candidate", "quality-policy-v3")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("regresses from pinned baseline");
        assertThat(store.findReleaseDecision(TENANT, "candidate")).isEmpty();
    }

    @Test
    void lowerIsBetterPolicyRejectsNumericRegressionAndAbsoluteThreshold() {
        store.createMetricPolicy(new EvaluationMetricPolicy(TENANT, "latency-policy-v1", List.of(
                new EvaluationMetricRule("p95_latency", "milliseconds",
                        EvaluationMetricDirection.LOWER_IS_BETTER, 200, 0))));
        store.createRun(runningRun("baseline", null, "model-v1", "latency-policy-v1"));
        store.completeRun(TENANT, "baseline", EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("p95_latency", 100, "milliseconds", true, null)),
                null, null, NOW.plusSeconds(5));
        store.createRun(runningRun("candidate", "baseline", "model-v2", "latency-policy-v1"));
        store.completeRun(TENANT, "candidate", EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("p95_latency", 120, "milliseconds", true, null)),
                null, null, NOW.plusSeconds(10));

        assertThatThrownBy(() -> store.recordReleaseDecision(approval("candidate", "latency-policy-v1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("regresses from pinned baseline");

        store.createRun(runningRun("above-limit", null, "model-v3", "latency-policy-v1"));
        store.completeRun(TENANT, "above-limit", EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("p95_latency", 210, "milliseconds", true, null)),
                null, null, NOW.plusSeconds(10));
        assertThatThrownBy(() -> store.recordReleaseDecision(approval("above-limit", "latency-policy-v1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("violates numerical threshold");
    }

    @Test
    void explicitRegressionAllowanceApprovesWithinToleranceButNotBelowThreshold() {
        store.createMetricPolicy(new EvaluationMetricPolicy(TENANT, "tolerant-policy-v1", List.of(
                new EvaluationMetricRule("quality", "ratio",
                        EvaluationMetricDirection.HIGHER_IS_BETTER, 0.8, 0.05))));
        store.createRun(runningRun("baseline", null, "model-v1", "tolerant-policy-v1"));
        store.completeRun(TENANT, "baseline", EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("quality", 0.90, "ratio", true, null)),
                null, null, NOW.plusSeconds(5));
        store.createRun(runningRun("within-tolerance", "baseline", "model-v2", "tolerant-policy-v1"));
        store.completeRun(TENANT, "within-tolerance", EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("quality", 0.88, "ratio", true, null)),
                null, null, NOW.plusSeconds(10));
        store.recordReleaseDecision(approval("within-tolerance", "tolerant-policy-v1"));
        assertThat(store.findReleaseDecision(TENANT, "within-tolerance").orElseThrow().approved())
                .isTrue();

        store.createRun(runningRun("below-threshold", null, "model-v3", "tolerant-policy-v1"));
        store.completeRun(TENANT, "below-threshold", EvaluationRunState.SUCCEEDED,
                List.of(new EvaluationMetric("quality", 0.79, "ratio", true, null)),
                null, null, NOW.plusSeconds(10));
        assertThatThrownBy(() -> store.recordReleaseDecision(
                approval("below-threshold", "tolerant-policy-v1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("violates numerical threshold");
    }

    @Test
    void policyVersionIsSealedAndRunCreationFailsClosedWithoutOne() throws Exception {
        EvaluationMetricPolicy policy = store.findMetricPolicy(TENANT, "quality-policy-v3")
                .orElseThrow();
        assertThat(policy.rules()).containsExactly(new EvaluationMetricRule(
                "quality", "ratio", EvaluationMetricDirection.HIGHER_IS_BETTER, 0.8, 0));
        assertThatThrownBy(() -> store.createMetricPolicy(policy))
                .isInstanceOf(AiControlException.class);
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE evaluation_metric_policy_rules SET threshold_value=0 "
                            + "WHERE policy_version='quality-policy-v3'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("immutable");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "INSERT INTO evaluation_metric_policy_rules "
                            + "(tenant_id,policy_version,metric_name,metric_unit,direction,"
                            + "threshold_value,maximum_baseline_regression) VALUES "
                            + "('tenant-a','quality-policy-v3','late-rule','ratio',"
                            + "'HIGHER_IS_BETTER',0.8,0)"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("sealed");
        }
        assertThatThrownBy(() -> store.createRun(
                runningRun("unregistered", null, "model-v2", "missing-policy")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("metric policy does not exist");
        assertThat(store.findRun(TENANT, "unregistered")).isEmpty();

        store.createRun(runningRun("sealed-run", null, "model-v2"));
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var result = statement.executeQuery(
                        "SELECT metric_policy_pinned FROM evaluation_runs WHERE run_id='sealed-run'")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getBoolean(1)).isTrue();
        }
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE evaluation_runs SET metric_policy_pinned=FALSE "
                            + "WHERE run_id='sealed-run'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("immutable");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE evaluation_runs SET policy_version='missing-policy' "
                            + "WHERE run_id='sealed-run'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("immutable");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE evaluation_runs SET baseline_run_id='sealed-run' "
                            + "WHERE run_id='sealed-run'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("immutable");
        }
    }

    @Test
    void nonFiniteAndNegativeNumericalRulesCannotBePublished() throws Exception {
        assertThatThrownBy(() -> new EvaluationMetricRule("quality", "ratio",
                EvaluationMetricDirection.HIGHER_IS_BETTER, Double.NaN, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("threshold must be finite");
        assertThatThrownBy(() -> new EvaluationMetricRule("quality", "ratio",
                EvaluationMetricDirection.HIGHER_IS_BETTER, 0.8, Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximumBaselineRegression");
        assertThatThrownBy(() -> new EvaluationMetricRule("quality", "ratio",
                EvaluationMetricDirection.HIGHER_IS_BETTER, 0.8, -0.01))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximumBaselineRegression");

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO evaluation_metric_policies "
                    + "(tenant_id,policy_version) VALUES ('tenant-a','invalid-policy')");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "INSERT INTO evaluation_metric_policy_rules "
                            + "(tenant_id,policy_version,metric_name,metric_unit,direction,"
                            + "threshold_value,maximum_baseline_regression) VALUES "
                            + "('tenant-a','invalid-policy','quality','ratio',"
                            + "'HIGHER_IS_BETTER','NaN'::DOUBLE PRECISION,0)"))
                    .isInstanceOf(SQLException.class);
        }
        assertThat(store.findMetricPolicy(TENANT, "invalid-policy")).isEmpty();
    }

    private double accuracyFromActualObservations(int correctCount) throws SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute("CREATE TEMP TABLE evaluation_observations "
                    + "(expected INTEGER NOT NULL, predicted INTEGER NOT NULL)");
            for (int i = 0; i < 10; i++) {
                statement.executeUpdate("INSERT INTO evaluation_observations VALUES (1, "
                        + (i < correctCount ? "1" : "0") + ")");
            }
            try (var rows = statement.executeQuery(
                    "SELECT AVG(CASE WHEN expected = predicted THEN 1.0 ELSE 0.0 END) "
                            + "FROM evaluation_observations")) {
                rows.next();
                return rows.getDouble(1);
            }
        }
    }

    private void assertSqlRejected(String sql, String message) {
        assertThatThrownBy(() -> {
            try (Connection connection = dataSource.getConnection();
                    PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.executeUpdate();
            }
        }).isInstanceOf(SQLException.class).hasMessageContaining(message);
    }

    private static EvaluationReleaseDecision approval(String runId, String policyVersion) {
        return new EvaluationReleaseDecision(TENANT, runId, true, policyVersion,
                "numerical metrics verified", "release-owner", NOW.plusSeconds(11));
    }

    private static EvaluationDatasetVersion dataset() {
        return new EvaluationDatasetVersion(
                new EvaluationDatasetId(TENANT, "golden-qa"),
                7,
                "evaluation-owner",
                "iceberg-snapshot-442",
                "s3://governed/evaluations/golden-qa/7/manifest.json",
                "a".repeat(64),
                1_000,
                "{\"input\":\"string\",\"expected\":\"string\"}",
                NOW);
    }

    private static EvaluationRun runningRun(
            String runId, String baselineRunId, String subjectVersion) {
        return runningRun(runId, baselineRunId, subjectVersion, "quality-policy-v3");
    }

    private static EvaluationRun runningRun(
            String runId, String baselineRunId, String subjectVersion, String policyVersion) {
        return new EvaluationRun(
                TENANT,
                runId,
                "golden-qa",
                7,
                EvaluationSubjectType.MODEL,
                "support-model",
                subjectVersion,
                baselineRunId,
                policyVersion,
                EvaluationRunState.RUNNING,
                NOW.plusSeconds(1),
                null,
                null,
                null);
    }
}
