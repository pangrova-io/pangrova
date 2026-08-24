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
import org.pangrova.parser.ast.statement.CreateEmbeddingModelStatement;
import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.security.UserIdentity;

/**
 * Executes CREATE EMBEDDING MODEL by registering the model in the
 * {@link EmbeddingModelRegistry}.
 */
public final class CreateEmbeddingModelTask implements MetadataTask<CreateEmbeddingModelStatement> {

    private final EmbeddingModelRegistry registry;

    public CreateEmbeddingModelTask(EmbeddingModelRegistry registry) {
        this.registry = registry;
    }

    @Override
    public StatementResult execute(CreateEmbeddingModelStatement statement,
                                   StatementExecutionContext context) {
        Map<String, String> props = statement.properties();
        String provider = props.getOrDefault("provider", "openai-compatible");
        String endpoint = props.getOrDefault("endpoint", "");
        int dimension = parseInt(props.getOrDefault("dimension", "0"));
        String apiKeySecret = props.getOrDefault("api_key_secret", "");
        int maxBatchSize = parseInt(props.getOrDefault("max_batch_size", "32"));
        int maxRps = parseInt(props.getOrDefault("max_requests_per_second", "100"));
        String queryPrefix = props.getOrDefault("query_prefix", "");
        String documentPrefix = props.getOrDefault("document_prefix", "");

        EmbeddingModelDescriptor descriptor = new EmbeddingModelDescriptor(
                statement.modelName(), provider, endpoint, dimension,
                apiKeySecret, maxBatchSize, maxRps,
                queryPrefix, documentPrefix, 0);

        UserIdentity user = context.session() != null ? context.session().userIdentity() : null;
        if (user == null) {
            user = new UserIdentity("system", java.util.Set.of("admin"),
                    Map.of(), java.time.Instant.now());
        }
        registry.create(descriptor, user);

        TypedRow row = TypedRow.builder()
                .putString("result", "Embedding model '" + statement.modelName() + "' created")
                .build();
        return new StatementResult(List.of(row), List.of("result"), null, StatementCategory.DDL);
    }

    private static int parseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
