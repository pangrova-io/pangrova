/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.ai.control.embedding.EmbeddingModelRegistry;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.DropEmbeddingModelStatement;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.security.api.UserIdentity;

/**
 * Routes DROP EMBEDDING MODEL to the registry. The registry currently rejects
 * both modes until generated-column dependencies can be fenced durably.
 */
public final class DropEmbeddingModelTask implements MetadataTask<DropEmbeddingModelStatement> {

    private final EmbeddingModelRegistry registry;

    public DropEmbeddingModelTask(EmbeddingModelRegistry registry) {
        this.registry = registry;
    }

    @Override
    public StatementResult execute(DropEmbeddingModelStatement statement,
                                   StatementExecutionContext context) {
        UserIdentity user = context.session() != null ? context.session().userIdentity() : null;
        if (user == null) {
            throw new PangrovaException(
                    PangrovaErrorCode.ACCESS_DENIED,
                    "DROP EMBEDDING MODEL requires an authenticated session");
        }
        registry.drop(statement.modelName(), statement.cascade(), user);

        TypedRow row = TypedRow.builder()
                .putString("result", "Embedding model '" + statement.modelName() + "' dropped")
                .build();
        return new StatementResult(List.of(row), List.of("result"), null, StatementCategory.DDL);
    }
}
