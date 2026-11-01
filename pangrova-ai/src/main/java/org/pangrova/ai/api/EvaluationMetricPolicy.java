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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Tenant-scoped, immutable metric rules selected by an evaluation run's policy version. */
public record EvaluationMetricPolicy(
        String tenantId,
        String version,
        List<EvaluationMetricRule> rules) {

    public EvaluationMetricPolicy {
        EvaluationValidation.requireText(tenantId, "tenantId");
        EvaluationValidation.requireText(version, "version");
        Objects.requireNonNull(rules, "rules must not be null");
        rules = List.copyOf(rules);
        if (rules.isEmpty()) {
            throw new IllegalArgumentException("metric policy requires at least one rule");
        }
        Set<String> names = new HashSet<>();
        for (EvaluationMetricRule rule : rules) {
            if (!names.add(rule.name())) {
                throw new IllegalArgumentException("duplicate metric rule: " + rule.name());
            }
        }
    }
}
