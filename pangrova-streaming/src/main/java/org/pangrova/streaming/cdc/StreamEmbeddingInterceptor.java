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
package org.pangrova.streaming.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Intercepts CDC write events to trigger embedding generation for tables
 * that have embedding-enabled columns configured.
 *
 * <p>When a CDC record arrives for a table with embedding configuration,
 * this interceptor extracts the text column value and enqueues an
 * embedding work item via the provided callback.</p>
 *
 * <p>Depends only on {@code pangrova-spi} interfaces — no coordinator or
 * worker imports per the module dependency rules.</p>
 */
public final class StreamEmbeddingInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamEmbeddingInterceptor.class);

    private final Map<String, EmbeddingColumnConfig> enabledTables;
    private final Consumer<EmbeddingWorkRequest> embeddingEnqueuer;

    /**
     * @param enabledTables    map of fully-qualified table name to embedding config
     * @param embeddingEnqueuer callback that receives the full embedding work context
     */
    public StreamEmbeddingInterceptor(
            Map<String, EmbeddingColumnConfig> enabledTables,
            Consumer<EmbeddingWorkRequest> embeddingEnqueuer) {
        this.enabledTables = Objects.requireNonNull(enabledTables);
        this.embeddingEnqueuer = Objects.requireNonNull(embeddingEnqueuer);
    }

    /**
     * Called after a CDC batch is committed. If the table has embedding
     * config, enqueues the committed file paths for embedding generation.
     *
     * @param tableName      fully qualified table name
     * @param committedFiles data file paths from the commit
     */
    public void onCommit(String tableName, List<String> committedFiles) {
        if (tableName == null) {
            return;
        }
        EmbeddingColumnConfig config = enabledTables.get(tableName);
        if (config == null) {
            return;
        }
        if (committedFiles == null || committedFiles.isEmpty()) {
            return;
        }
        LOG.info("event=stream_embedding_triggered table={} files={} model={}",
                tableName, committedFiles.size(), config.modelName());
        embeddingEnqueuer.accept(new EmbeddingWorkRequest(
                tableName, config.textColumn(), config.modelName(),
                config.vectorColumn(), List.copyOf(committedFiles)));
    }

    public Set<String> enabledTableNames() {
        return Collections.unmodifiableSet(enabledTables.keySet());
    }

    /**
     * Configuration for a table's embedding column.
     *
     * @param textColumn   column containing text to embed
     * @param modelName    registered embedding model name
     * @param vectorColumn target vector column name
     */
    public record EmbeddingColumnConfig(
            String textColumn, String modelName, String vectorColumn) {
        public EmbeddingColumnConfig {
            Objects.requireNonNull(textColumn);
            Objects.requireNonNull(modelName);
            Objects.requireNonNull(vectorColumn);
        }
    }

    /**
     * Full context passed to the embedding enqueuer so downstream processors
     * know which column to read, which model to call, and where to store the
     * resulting vector.
     *
     * @param tableName    fully qualified source table name
     * @param textColumn   column whose text content should be embedded
     * @param modelName    registered embedding model name
     * @param vectorColumn target vector column to write the embedding into
     * @param filePaths    data file paths from the CDC commit
     */
    public record EmbeddingWorkRequest(
            String tableName,
            String textColumn,
            String modelName,
            String vectorColumn,
            List<String> filePaths) {
        public EmbeddingWorkRequest {
            Objects.requireNonNull(tableName);
            Objects.requireNonNull(textColumn);
            Objects.requireNonNull(modelName);
            Objects.requireNonNull(vectorColumn);
            filePaths = List.copyOf(Objects.requireNonNull(filePaths));
        }
    }
}
