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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.ShowEmbeddingModelsStatement;
import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.security.UserIdentity;

/**
 * Executes SHOW EMBEDDING MODELS by listing all models from the
 * {@link EmbeddingModelRegistry}.
 */
public final class ShowEmbeddingModelsTask implements MetadataTask<ShowEmbeddingModelsStatement> {

    private static final List<String> COLUMN_NAMES = List.of(
            "name", "provider", "endpoint", "dimension", "max_batch_size", "version");

    private final EmbeddingModelRegistry registry;

    public ShowEmbeddingModelsTask(EmbeddingModelRegistry registry) {
        this.registry = registry;
    }

    @Override
    public StatementResult execute(ShowEmbeddingModelsStatement statement,
                                   StatementExecutionContext context) {
        UserIdentity user = context.session() != null ? context.session().userIdentity() : null;
        if (user == null) {
            user = new UserIdentity("system", java.util.Set.of("admin", "users"),
                    Map.of(), java.time.Instant.now());
        }
        List<EmbeddingModelDescriptor> models = registry.listAll(user);
        List<TypedRow> rows = new ArrayList<>(models.size());
        for (EmbeddingModelDescriptor model : models) {
            rows.add(TypedRow.builder()
                    .putString("name", model.name())
                    .putString("provider", model.provider())
                    .putString("endpoint", model.endpoint())
                    .putInt("dimension", model.dimension())
                    .putInt("max_batch_size", model.maxBatchSize())
                    .putInt("version", model.version())
                    .build());
        }
        return new StatementResult(rows, COLUMN_NAMES, null, StatementCategory.SHOW);
    }
}
