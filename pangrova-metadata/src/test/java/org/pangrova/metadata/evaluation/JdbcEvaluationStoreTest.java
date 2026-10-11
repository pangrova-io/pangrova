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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.ai.AiInfrastructureStores;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcEvaluationStoreTest {

    private static final String TENANT = "tenant-a";
    private static final Instant NOW = Instant.parse("2026-09-19T00:00:00Z");

    private EvaluationStore store;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("evaluation_store");
        store = AiInfrastructureStores.jdbc(dataSource).evaluations();
        store.createDatasetVersion(dataset());
    }

    @Test
    void pinsDatasetAndBaselineAndPersistsAReleaseGate() {
        store.createRun(runningRun("baseline", null, "model-v1"));
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

        store.createRun(runningRun("candidate", "baseline", "model-v2"));
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
                "quality-policy-v3",
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
                .isInstanceOf(MetastoreException.class);

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
                .isInstanceOf(MetastoreException.class);
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
        return new EvaluationRun(
                TENANT,
                runId,
                "golden-qa",
                7,
                EvaluationSubjectType.MODEL,
                "support-model",
                subjectVersion,
                baselineRunId,
                "quality-policy-v3",
                EvaluationRunState.RUNNING,
                NOW.plusSeconds(1),
                null,
                null,
                null);
    }
}
