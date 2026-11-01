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
package org.pangrova.ai.control;

import org.pangrova.ai.api.EvaluationStore;
import org.pangrova.ai.store.jdbc.JdbcEvaluationStore;
import org.pangrova.semantic.store.jdbc.JdbcSemanticViewStore;
import org.pangrova.semantic.api.SemanticViewStore;
import org.pangrova.ai.api.FeatureCatalogStore;
import org.pangrova.ai.store.jdbc.JdbcFeatureCatalogStore;

import javax.sql.DataSource;
import java.util.Objects;

/** Production metadata entry point for governed AI infrastructure. */
public record AiInfrastructureStores(
        SemanticViewStore semanticViews,
        EvaluationStore evaluations,
        FeatureCatalogStore features) {

    public AiInfrastructureStores {
        Objects.requireNonNull(semanticViews, "semanticViews must not be null");
        Objects.requireNonNull(evaluations, "evaluations must not be null");
        Objects.requireNonNull(features, "features must not be null");
    }

    /** Creates the governed metadata stores against the supplied durable database. */
    public static AiInfrastructureStores jdbc(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource must not be null");
        return new AiInfrastructureStores(
                new JdbcSemanticViewStore(dataSource),
                new JdbcEvaluationStore(dataSource),
                new JdbcFeatureCatalogStore(dataSource));
    }
}
