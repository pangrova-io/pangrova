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
package org.pangrova.coordinator.query.execution.dispatch.task.semanticview;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.parser.api.ast.statement.ShowSemanticViewsStatement;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.semantic.api.version.SemanticViewOperations;
import org.pangrova.types.value.TypedRow;

import java.util.List;
import java.util.Objects;

/** Lists governed Semantic View versions visible in an authorized tenant schema. */
public final class ShowSemanticViewsTask implements MetadataTask<ShowSemanticViewsStatement> {

    private static final List<String> COLUMNS = List.of(
            "catalog_name", "schema_name", "view_name", "version", "active",
            "active_version", "publication_epoch", "validation_state", "owner", "created_at");

    private final SemanticViewOperations service;

    public ShowSemanticViewsTask(SemanticViewOperations service) {
        this.service = Objects.requireNonNull(service, "service must not be null");
    }

    @Override
    public StatementResult execute(
            ShowSemanticViewsStatement statement,
            StatementExecutionContext context) {
        var scope = SemanticViewSqlScope.from(context.session()).resolveSchema(statement.scope());
        List<TypedRow> rows = service.show(scope.user(), scope.tenantId(), scope.catalog(), scope.schema()).stream()
                .map(status -> TypedRow.builder()
                        .putString("catalog_name", status.version().id().catalogName())
                        .putString("schema_name", status.version().id().schemaName())
                        .putString("view_name", status.version().id().viewName())
                        .putLong("version", status.version().version())
                        .putRaw("active", status.active())
                        .putRaw("active_version", status.activeVersion().isPresent()
                                ? status.activeVersion().getAsLong() : null)
                        .putLong("publication_epoch", status.publicationEpoch())
                        .putString("validation_state", status.version().validationState().name())
                        .putString("owner", status.version().ownerPrincipal())
                        .putString("created_at", status.version().createdAt().toString())
                        .build())
                .toList();
        return new StatementResult(rows, COLUMNS, null, StatementCategory.SHOW);
    }
}
