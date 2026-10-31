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
package org.pangrova.ai.control.assembly;

import org.pangrova.ai.control.evaluation.EvaluationStore;
import org.pangrova.ai.control.feature.FeatureCatalogStore;
import org.pangrova.ai.control.feature.CatalogExportBuildStore;
import org.pangrova.ai.control.store.jdbc.evaluation.JdbcEvaluationStore;
import org.pangrova.ai.control.store.jdbc.feature.JdbcFeatureCatalogStore;
import org.pangrova.ai.control.store.jdbc.feature.JdbcCatalogExportBuildStore;
import org.pangrova.generation.spi.jdbc.GenerationJobSubmissionTransactions;

import javax.sql.DataSource;
import java.util.Objects;

/** Production entry point for governed AI control-plane infrastructure. */
public record AiInfrastructureStores(
        EvaluationStore evaluations,
        FeatureCatalogStore features,
        CatalogExportBuildStore catalogExports) {

    public AiInfrastructureStores {
        Objects.requireNonNull(evaluations, "evaluations must not be null");
        Objects.requireNonNull(features, "features must not be null");
        Objects.requireNonNull(catalogExports, "catalogExports must not be null");
    }

    /** Creates the governed AI control stores against the supplied durable database. */
    public static AiInfrastructureStores jdbc(
            DataSource dataSource, GenerationJobSubmissionTransactions jobSubmissions) {
        Objects.requireNonNull(dataSource, "dataSource must not be null");
        Objects.requireNonNull(jobSubmissions, "jobSubmissions must not be null");
        return new AiInfrastructureStores(
                new JdbcEvaluationStore(dataSource),
                new JdbcFeatureCatalogStore(dataSource),
                new JdbcCatalogExportBuildStore(dataSource, jobSubmissions));
    }
}
