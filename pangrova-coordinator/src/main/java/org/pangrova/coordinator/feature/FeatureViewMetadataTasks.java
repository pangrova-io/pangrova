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
package org.pangrova.coordinator.feature;

import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.CreateFeatureViewStatement;
import org.pangrova.parser.ast.statement.MaterializeFeatureViewStatement;
import org.pangrova.parser.ast.statement.PublishFeatureViewStatement;
import org.pangrova.spi.connector.data.TypedRow;

import java.util.List;
import java.util.Objects;

/** Registers Feature View metadata statements without adding another dispatch path. */
public final class FeatureViewMetadataTasks {

    private FeatureViewMetadataTasks() {}

    /** Installs handlers in the existing statement dispatcher, preserving each authenticated execution context. */
    public static void register(
            MetadataExecutionFactory factory,
            FeatureViewStatementService service) {
        Objects.requireNonNull(factory, "factory must not be null");
        Objects.requireNonNull(service, "service must not be null");
        factory.registerTask(CreateFeatureViewStatement.class,
                (MetadataTask<CreateFeatureViewStatement>) (statement, context) -> {
                    var version = service.create(statement, context.session());
                    return row(List.of("catalog_name", "schema_name", "view_name", "version"),
                            TypedRow.builder()
                                    .putString("catalog_name", version.id().catalogName())
                                    .putString("schema_name", version.id().schemaName())
                                    .putString("view_name", version.id().viewName())
                                    .putLong("version", version.version()).build());
                });
        factory.registerTask(PublishFeatureViewStatement.class,
                (MetadataTask<PublishFeatureViewStatement>) (statement, context) -> {
                    var publication = service.publish(statement, context.session());
                    return row(List.of("catalog_name", "schema_name", "view_name",
                                    "active_version", "publication_epoch"),
                            TypedRow.builder()
                                    .putString("catalog_name", publication.id().catalogName())
                                    .putString("schema_name", publication.id().schemaName())
                                    .putString("view_name", publication.id().viewName())
                                    .putLong("active_version", publication.activeVersion())
                                    .putLong("publication_epoch", publication.publicationEpoch())
                                    .build());
                });
        factory.registerTask(MaterializeFeatureViewStatement.class,
                (MetadataTask<MaterializeFeatureViewStatement>) (statement, context) -> {
                    var export = service.materialize(statement, context.session());
                    return row(List.of("job_id", "artifact_id", "generation"),
                            TypedRow.builder()
                                    .putString("job_id", export.jobId())
                                    .putString("artifact_id", export.artifactId())
                                    .putString("generation", export.generation()).build());
                });
    }

    private static StatementResult row(List<String> columns, TypedRow row) {
        return new StatementResult(
                List.of(row), columns, null, StatementCategory.DDL);
    }
}
