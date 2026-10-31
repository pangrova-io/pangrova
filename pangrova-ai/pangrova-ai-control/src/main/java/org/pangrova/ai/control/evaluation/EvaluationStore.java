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
package org.pangrova.ai.control.evaluation;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Durable contract for evaluation datasets, runs, evidence, and release gates. */
public interface EvaluationStore {

    /** Publishes one immutable tenant-scoped policy version and all of its metric rules. */
    void createMetricPolicy(EvaluationMetricPolicy policy);

    Optional<EvaluationMetricPolicy> findMetricPolicy(String tenantId, String version);

    void createDatasetVersion(EvaluationDatasetVersion dataset);

    Optional<EvaluationDatasetVersion> findDatasetVersion(
            EvaluationDatasetId id, long version);

    void createRun(EvaluationRun run);

    Optional<EvaluationRun> findRun(String tenantId, String runId);

    /**
     * Atomically moves a running evaluation to one terminal state and publishes its metrics.
     * Returns false if another writer already completed or cancelled the run.
     */
    boolean completeRun(
            String tenantId,
            String runId,
            EvaluationRunState outcome,
            List<EvaluationMetric> metrics,
            String traceReference,
            String failureClass,
            Instant completedAt);

    /**
     * Atomically records the row count of a published Feature View generation.
     * This is a materialization-volume check, not a model-quality evaluation.
     * The run must pin that generation and its exact published manifest as its dataset.
     */
    boolean completeFeatureViewMaterializationRun(
            String tenantId, String runId, Instant completedAt);

    List<EvaluationMetric> listMetrics(String tenantId, String runId);

    /** Records the one immutable release decision, enforcing policy and metric gates. */
    void recordReleaseDecision(EvaluationReleaseDecision decision);

    Optional<EvaluationReleaseDecision> findReleaseDecision(
            String tenantId, String runId);
}
