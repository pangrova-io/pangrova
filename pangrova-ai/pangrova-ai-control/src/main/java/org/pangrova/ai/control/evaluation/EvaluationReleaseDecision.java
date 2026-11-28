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
import java.util.Objects;

import static org.pangrova.common.validation.Preconditions.requireText;

/** Immutable release-gate decision tied to a completed, version-pinned run. */
public record EvaluationReleaseDecision(
        String tenantId,
        String runId,
        boolean approved,
        String policyVersion,
        String reason,
        String decidedBy,
        Instant decidedAt) {

    public EvaluationReleaseDecision {
        requireText(tenantId, "tenantId");
        requireText(runId, "runId");
        requireText(policyVersion, "policyVersion");
        requireText(reason, "reason");
        requireText(decidedBy, "decidedBy");
        Objects.requireNonNull(decidedAt, "decidedAt must not be null");
    }
}
