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
package org.pangrova.streaming.core.cdc;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StreamEmbeddingInterceptorTest {

    private static final String TABLE = "cat.schema.documents";
    private static final StreamEmbeddingInterceptor.EmbeddingColumnConfig CONFIG =
            new StreamEmbeddingInterceptor.EmbeddingColumnConfig("body", "text-embed-v2", "body_vec");

    @Test
    void constructorRejectsNullEnabledTables() {
        assertThrows(NullPointerException.class,
                () -> new StreamEmbeddingInterceptor(null, req -> {}));
    }

    @Test
    void constructorRejectsNullEnqueuer() {
        assertThrows(NullPointerException.class,
                () -> new StreamEmbeddingInterceptor(Map.of(TABLE, CONFIG), null));
    }

    @Test
    void onCommit_enqueuedForEnabledTable() {
        List<StreamEmbeddingInterceptor.EmbeddingWorkRequest> captured = new ArrayList<>();
        StreamEmbeddingInterceptor interceptor = new StreamEmbeddingInterceptor(
                Map.of(TABLE, CONFIG), captured::add);

        interceptor.onCommit(TABLE, List.of("s3://bucket/part-0001.parquet"));

        assertEquals(1, captured.size());
        StreamEmbeddingInterceptor.EmbeddingWorkRequest req = captured.get(0);
        assertEquals(TABLE, req.tableName());
        assertEquals("body", req.textColumn());
        assertEquals("text-embed-v2", req.modelName());
        assertEquals("body_vec", req.vectorColumn());
        assertEquals(List.of("s3://bucket/part-0001.parquet"), req.filePaths());
    }

    @Test
    void onCommit_ignoredForDisabledTable() {
        List<StreamEmbeddingInterceptor.EmbeddingWorkRequest> captured = new ArrayList<>();
        StreamEmbeddingInterceptor interceptor = new StreamEmbeddingInterceptor(
                Map.of(TABLE, CONFIG), captured::add);

        interceptor.onCommit("cat.schema.other_table", List.of("file.parquet"));

        assertTrue(captured.isEmpty(), "Should not enqueue for tables without embedding config");
    }

    @Test
    void onCommit_ignoredForNullTable() {
        List<StreamEmbeddingInterceptor.EmbeddingWorkRequest> captured = new ArrayList<>();
        StreamEmbeddingInterceptor interceptor = new StreamEmbeddingInterceptor(
                Map.of(TABLE, CONFIG), captured::add);

        interceptor.onCommit(null, List.of("file.parquet"));

        assertTrue(captured.isEmpty());
    }

    @Test
    void onCommit_ignoredForEmptyFiles() {
        List<StreamEmbeddingInterceptor.EmbeddingWorkRequest> captured = new ArrayList<>();
        StreamEmbeddingInterceptor interceptor = new StreamEmbeddingInterceptor(
                Map.of(TABLE, CONFIG), captured::add);

        interceptor.onCommit(TABLE, List.of());

        assertTrue(captured.isEmpty(), "Should not enqueue when no files committed");
    }

    @Test
    void onCommit_enqueuedWithMultipleFiles() {
        List<StreamEmbeddingInterceptor.EmbeddingWorkRequest> captured = new ArrayList<>();
        StreamEmbeddingInterceptor interceptor = new StreamEmbeddingInterceptor(
                Map.of(TABLE, CONFIG), captured::add);

        List<String> files = List.of("file1.parquet", "file2.parquet", "file3.parquet");
        interceptor.onCommit(TABLE, files);

        assertEquals(1, captured.size());
        assertEquals(files, captured.get(0).filePaths());
    }

    @Test
    void enabledTableNames_reflectsConfig() {
        StreamEmbeddingInterceptor interceptor = new StreamEmbeddingInterceptor(
                Map.of(TABLE, CONFIG, "other.schema.tbl",
                        new StreamEmbeddingInterceptor.EmbeddingColumnConfig("x", "m", "v")),
                req -> {});

        assertTrue(interceptor.enabledTableNames().contains(TABLE));
        assertEquals(2, interceptor.enabledTableNames().size());
    }

    @Test
    void embeddingColumnConfig_rejectsNulls() {
        assertThrows(NullPointerException.class,
                () -> new StreamEmbeddingInterceptor.EmbeddingColumnConfig(null, "m", "v"));
        assertThrows(NullPointerException.class,
                () -> new StreamEmbeddingInterceptor.EmbeddingColumnConfig("col", null, "v"));
        assertThrows(NullPointerException.class,
                () -> new StreamEmbeddingInterceptor.EmbeddingColumnConfig("col", "m", null));
    }
}
