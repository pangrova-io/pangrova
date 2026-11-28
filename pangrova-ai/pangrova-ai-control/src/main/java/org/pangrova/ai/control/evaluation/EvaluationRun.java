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

import org.pangrova.ai.control.enums.EvaluationRunState;
import org.pangrova.ai.control.enums.EvaluationSubjectType;

import java.time.Instant;
import java.util.Objects;

import static org.pangrova.common.validation.Preconditions.requireOptionalText;
import static org.pangrova.common.validation.Preconditions.requireText;

/** Version-pinned evaluation execution and its single terminal outcome. */
public record EvaluationRun(
        String tenantId,
        String runId,
        String datasetId,
        long datasetVersion,
        EvaluationSubjectType subjectType,
        String subjectId,
        String subjectVersion,
        String baselineRunId,
        String policyVersion,
        EvaluationRunState state,
        Instant startedAt,
        Instant completedAt,
        String traceReference,
        String failureClass) {

    public EvaluationRun {
        requireText(tenantId, "tenantId");
        requireText(runId, "runId");
        requireText(datasetId, "datasetId");
        if (datasetVersion <= 0) {
            throw new IllegalArgumentException("datasetVersion must be positive");
        }
        Objects.requireNonNull(subjectType, "subjectType must not be null");
        requireText(subjectId, "subjectId");
        requireText(subjectVersion, "subjectVersion");
        requireOptionalText(baselineRunId, "baselineRunId");
        if (runId.equals(baselineRunId)) {
            throw new IllegalArgumentException("a run cannot use itself as its baseline");
        }
        requireText(policyVersion, "policyVersion");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(startedAt, "startedAt must not be null");
        requireOptionalText(traceReference, "traceReference");
        requireOptionalText(failureClass, "failureClass");
        if (!state.isTerminal()) {
            if (completedAt != null || failureClass != null) {
                throw new IllegalArgumentException(
                        "running evaluation must not have terminal fields");
            }
        } else if (completedAt == null) {
            throw new IllegalArgumentException("terminal evaluation requires completedAt");
        } else if (state == EvaluationRunState.SUCCEEDED && failureClass != null) {
            throw new IllegalArgumentException(
                    "successful evaluation must not have a failureClass");
        } else if (state == EvaluationRunState.FAILED && failureClass == null) {
            throw new IllegalArgumentException("failed evaluation requires failureClass");
        }
        if (completedAt != null && completedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("completedAt must not precede startedAt");
        }
    }
}
