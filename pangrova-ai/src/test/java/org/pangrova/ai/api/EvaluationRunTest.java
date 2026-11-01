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
package org.pangrova.ai.api;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.EvaluationRun;
import org.pangrova.ai.api.EvaluationRunState;
import org.pangrova.ai.api.EvaluationSubjectType;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EvaluationRunTest {

    private static final Instant STARTED = Instant.parse("2026-10-07T00:00:00Z");

    @Test
    void rejectsCompletionBeforeStartForAllTerminalStates() {
        for (EvaluationRunState state : List.of(
                EvaluationRunState.SUCCEEDED, EvaluationRunState.FAILED, EvaluationRunState.CANCELLED)) {
            assertThatThrownBy(() -> terminal(state, STARTED.minusNanos(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("completedAt must not precede startedAt");
        }
    }

    @Test
    void acceptsEqualAndLaterCompletionInstants() {
        for (EvaluationRunState state : List.of(
                EvaluationRunState.SUCCEEDED, EvaluationRunState.FAILED, EvaluationRunState.CANCELLED)) {
            for (Instant completed : List.of(STARTED, STARTED.plusNanos(1))) {
                assertThat(terminal(state, completed).completedAt()).isEqualTo(completed);
            }
        }
    }

    private static EvaluationRun terminal(EvaluationRunState state, Instant completed) {
        return new EvaluationRun("tenant", "run", "dataset", 1, EvaluationSubjectType.MODEL,
                "model", "v1", null, "policy-v1", state, STARTED, completed, null,
                state == EvaluationRunState.FAILED ? "execution-failed" : null);
    }
}
