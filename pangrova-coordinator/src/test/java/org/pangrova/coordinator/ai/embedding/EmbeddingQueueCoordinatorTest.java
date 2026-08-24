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
package org.pangrova.coordinator.ai.embedding;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingQueueCoordinatorTest {

    private static final String TABLE = "catalog.schema.documents";
    private static final String MODEL = "bge_m3";
    private static final String VECTOR_COL = "embedding";

    private EmbeddingQueueCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new EmbeddingQueueCoordinator();
    }

    @Test
    void enqueueIncreasesQueueDepth() {
        coordinator.enqueue(TABLE, "row-1", MODEL, VECTOR_COL, "hello world");
        coordinator.enqueue(TABLE, "row-2", MODEL, VECTOR_COL, "another text");

        assertThat(coordinator.queueDepth(TABLE)).isEqualTo(2);
    }

    @Test
    void enqueueIsolatesTableQueues() {
        String otherTable = "catalog.schema.products";
        coordinator.enqueue(TABLE, "row-1", MODEL, VECTOR_COL, "text");
        coordinator.enqueue(otherTable, "row-1", MODEL, VECTOR_COL, "other");

        assertThat(coordinator.queueDepth(TABLE)).isEqualTo(1);
        assertThat(coordinator.queueDepth(otherTable)).isEqualTo(1);
    }

    @Test
    void drainReturnsEnqueuedRowsInOrder() {
        coordinator.enqueue(TABLE, "row-1", MODEL, VECTOR_COL, "first");
        coordinator.enqueue(TABLE, "row-2", MODEL, VECTOR_COL, "second");
        coordinator.enqueue(TABLE, "row-3", MODEL, VECTOR_COL, "third");

        List<PendingEmbeddingRow> batch = coordinator.drain(TABLE, 2);

        assertThat(batch).hasSize(2);
        assertThat(batch.get(0).rowId()).isEqualTo("row-1");
        assertThat(batch.get(1).rowId()).isEqualTo("row-2");
        assertThat(coordinator.queueDepth(TABLE)).isEqualTo(1);
    }

    @Test
    void drainReturnsEmptyForUnknownTable() {
        List<PendingEmbeddingRow> batch = coordinator.drain("unknown.table", 10);
        assertThat(batch).isEmpty();
    }

    @Test
    void drainReturnsEmptyWhenQueueExhausted() {
        coordinator.enqueue(TABLE, "row-1", MODEL, VECTOR_COL, "text");
        coordinator.drain(TABLE, 10);

        List<PendingEmbeddingRow> batch = coordinator.drain(TABLE, 10);
        assertThat(batch).isEmpty();
    }

    @Test
    void drainRespectsMaxRows() {
        for (int i = 0; i < 10; i++) {
            coordinator.enqueue(TABLE, "row-" + i, MODEL, VECTOR_COL, "text-" + i);
        }

        List<PendingEmbeddingRow> batch = coordinator.drain(TABLE, 3);
        assertThat(batch).hasSize(3);
        assertThat(coordinator.queueDepth(TABLE)).isEqualTo(7);
    }

    @Test
    void backpressureThrowsWhenThresholdReached() {
        EmbeddingQueueCoordinator smallQueue = new EmbeddingQueueCoordinator(5);

        for (int i = 0; i < 5; i++) {
            smallQueue.enqueue(TABLE, "row-" + i, MODEL, VECTOR_COL, "text");
        }

        assertThatThrownBy(() -> smallQueue.enqueue(TABLE, "row-5", MODEL, VECTOR_COL, "text"))
                .isInstanceOf(EmbeddingQueueFullException.class)
                .hasMessageContaining("backpressure")
                .hasMessageContaining(TABLE);
    }

    @Test
    void backpressureReleasesAfterDrain() {
        EmbeddingQueueCoordinator smallQueue = new EmbeddingQueueCoordinator(5);

        for (int i = 0; i < 5; i++) {
            smallQueue.enqueue(TABLE, "row-" + i, MODEL, VECTOR_COL, "text");
        }

        smallQueue.drain(TABLE, 2);

        // Should succeed now that queue is below threshold
        smallQueue.enqueue(TABLE, "row-new", MODEL, VECTOR_COL, "text");
        assertThat(smallQueue.queueDepth(TABLE)).isEqualTo(4);
    }

    @Test
    void isBackpressureActiveReflectsThreshold() {
        EmbeddingQueueCoordinator smallQueue = new EmbeddingQueueCoordinator(3);

        assertThat(smallQueue.isBackpressureActive(TABLE)).isFalse();

        for (int i = 0; i < 3; i++) {
            smallQueue.enqueue(TABLE, "row-" + i, MODEL, VECTOR_COL, "text");
        }

        assertThat(smallQueue.isBackpressureActive(TABLE)).isTrue();
    }

    @Test
    void queryQueueStatusReturnsAllTablesWithPendingRows() {
        String table2 = "catalog.schema.products";
        coordinator.enqueue(TABLE, "row-1", MODEL, VECTOR_COL, "text");
        coordinator.enqueue(TABLE, "row-2", MODEL, VECTOR_COL, "text");
        coordinator.enqueue(table2, "row-1", MODEL, VECTOR_COL, "text");

        List<EmbeddingQueueStatus> statuses = coordinator.queryQueueStatus();

        assertThat(statuses).hasSize(2);
        assertThat(statuses).extracting(EmbeddingQueueStatus::tableName)
                .containsExactlyInAnyOrder(TABLE, table2);

        EmbeddingQueueStatus docStatus = statuses.stream()
                .filter(s -> s.tableName().equals(TABLE))
                .findFirst().orElseThrow();
        assertThat(docStatus.pendingRows()).isEqualTo(2);
        assertThat(docStatus.oldestEnqueueTime()).isNotNull();
        assertThat(docStatus.backpressureActive()).isFalse();
    }

    @Test
    void queryQueueStatusExcludesEmptyQueues() {
        coordinator.enqueue(TABLE, "row-1", MODEL, VECTOR_COL, "text");
        coordinator.drain(TABLE, 10);

        List<EmbeddingQueueStatus> statuses = coordinator.queryQueueStatus();
        assertThat(statuses).isEmpty();
    }

    @Test
    void queryQueueStatusShowsBackpressureFlag() {
        EmbeddingQueueCoordinator smallQueue = new EmbeddingQueueCoordinator(3);
        for (int i = 0; i < 3; i++) {
            smallQueue.enqueue(TABLE, "row-" + i, MODEL, VECTOR_COL, "text");
        }

        List<EmbeddingQueueStatus> statuses = smallQueue.queryQueueStatus();
        assertThat(statuses).hasSize(1);
        assertThat(statuses.get(0).backpressureActive()).isTrue();
    }

    @Test
    void queueDepthReturnsZeroForUnknownTable() {
        assertThat(coordinator.queueDepth("nonexistent.table")).isEqualTo(0);
    }

    @Test
    void defaultBackpressureThresholdIs100K() {
        assertThat(coordinator.backpressureThreshold()).isEqualTo(100_000L);
    }

    @Test
    void constructorRejectsNonPositiveThreshold() {
        assertThatThrownBy(() -> new EmbeddingQueueCoordinator(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EmbeddingQueueCoordinator(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void enqueueRejectsNullArguments() {
        assertThatThrownBy(() -> coordinator.enqueue(null, "r", MODEL, VECTOR_COL, "t"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> coordinator.enqueue(TABLE, null, MODEL, VECTOR_COL, "t"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> coordinator.enqueue(TABLE, "r", null, VECTOR_COL, "t"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> coordinator.enqueue(TABLE, "r", MODEL, null, "t"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> coordinator.enqueue(TABLE, "r", MODEL, VECTOR_COL, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void drainRejectsInvalidArguments() {
        assertThatThrownBy(() -> coordinator.drain(null, 10))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> coordinator.drain(TABLE, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> coordinator.drain(TABLE, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pendingEmbeddingRowPreservesAllFields() {
        coordinator.enqueue(TABLE, "row-42", "clip_vit", "image_vec", "shoe image");

        List<PendingEmbeddingRow> batch = coordinator.drain(TABLE, 1);
        PendingEmbeddingRow row = batch.get(0);

        assertThat(row.tableName()).isEqualTo(TABLE);
        assertThat(row.rowId()).isEqualTo("row-42");
        assertThat(row.modelName()).isEqualTo("clip_vit");
        assertThat(row.vectorColumn()).isEqualTo("image_vec");
        assertThat(row.sourceText()).isEqualTo("shoe image");
        assertThat(row.enqueuedAt()).isNotNull();
    }

    @Test
    void embeddingQueueFullExceptionCarriesContext() {
        EmbeddingQueueCoordinator smallQueue = new EmbeddingQueueCoordinator(2);
        smallQueue.enqueue(TABLE, "row-0", MODEL, VECTOR_COL, "text");
        smallQueue.enqueue(TABLE, "row-1", MODEL, VECTOR_COL, "text");

        try {
            smallQueue.enqueue(TABLE, "row-2", MODEL, VECTOR_COL, "text");
        } catch (EmbeddingQueueFullException e) {
            assertThat(e.tableName()).isEqualTo(TABLE);
            assertThat(e.currentDepth()).isEqualTo(2);
            assertThat(e.threshold()).isEqualTo(2);
            return;
        }
        throw new AssertionError("Expected EmbeddingQueueFullException");
    }
}
