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
import java.util.Map;

import org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.DropEmbeddingModelStatement;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.security.UserIdentity;

/**
 * Executes DROP EMBEDDING MODEL by removing the model from the
 * {@link EmbeddingModelRegistry}.
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
            user = new UserIdentity("system", java.util.Set.of("admin"),
                    Map.of(), java.time.Instant.now());
        }
        registry.drop(statement.modelName(), statement.cascade(), user);

        TypedRow row = TypedRow.builder()
                .putString("result", "Embedding model '" + statement.modelName() + "' dropped")
                .build();
        return new StatementResult(List.of(row), List.of("result"), null, StatementCategory.DDL);
    }
}
