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
package org.pangrova.coordinator.query.execution.dispatch.task.semantic;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.semantic.SemanticViewStatementService;
import org.pangrova.parser.ast.statement.CreateSemanticViewStatement;
import org.pangrova.connector.spi.data.value.TypedRow;

import java.util.List;
import java.util.Objects;

/** Executes metadata-only CREATE SEMANTIC VIEW. */
public final class CreateSemanticViewTask implements MetadataTask<CreateSemanticViewStatement> {

    private static final List<String> COLUMNS = List.of(
            "catalog_name", "schema_name", "view_name", "version", "validation_state");

    private final SemanticViewStatementService service;

    public CreateSemanticViewTask(SemanticViewStatementService service) {
        this.service = Objects.requireNonNull(service, "service must not be null");
    }

    @Override
    public StatementResult execute(
            CreateSemanticViewStatement statement,
            StatementExecutionContext context) {
        var version = service.create(statement, context.session());
        TypedRow row = TypedRow.builder()
                .putString("catalog_name", version.id().catalogName())
                .putString("schema_name", version.id().schemaName())
                .putString("view_name", version.id().viewName())
                .putLong("version", version.version())
                .putString("validation_state", version.validationState().name())
                .build();
        return new StatementResult(List.of(row), COLUMNS, null, StatementCategory.DDL);
    }
}
