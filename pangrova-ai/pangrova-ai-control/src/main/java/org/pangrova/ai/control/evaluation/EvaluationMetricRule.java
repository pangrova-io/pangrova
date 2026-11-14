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

import org.pangrova.ai.control.enums.EvaluationMetricDirection;

import java.util.Objects;

import static org.pangrova.common.util.Preconditions.requireText;

/** Immutable numerical release rule for one named metric. */
public record EvaluationMetricRule(
        String name,
        String unit,
        EvaluationMetricDirection direction,
        double threshold,
        double maximumBaselineRegression) {

    public EvaluationMetricRule {
        requireText(name, "name");
        requireText(unit, "unit");
        Objects.requireNonNull(direction, "direction must not be null");
        if (!Double.isFinite(threshold)) {
            throw new IllegalArgumentException("threshold must be finite");
        }
        if (!Double.isFinite(maximumBaselineRegression)
                || maximumBaselineRegression < 0) {
            throw new IllegalArgumentException(
                    "maximumBaselineRegression must be finite and nonnegative");
        }
    }
}
