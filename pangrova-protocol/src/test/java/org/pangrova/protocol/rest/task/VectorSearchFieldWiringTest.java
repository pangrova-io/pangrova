package org.pangrova.protocol.rest.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

/**
 * Verifies that vector search parameters flow as a first-class field on
 * {@link SubmitTaskRequest} rather than being smuggled through Base64-encoded
 * session properties (F-CHAIN-INDEX-06).
 *
 * <p>Tests cover: record accessor, JSON round-trip, backward-compatible
 * constructors (null vectorSearch), and field semantics.</p>
 */
class VectorSearchFieldWiringTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void vectorSearchFieldAccessor() {
        VectorSearchInfoPayload vs = samplePayload();
        SubmitTaskRequest req = requestWithVectorSearch(vs);

        assertThat(req.vectorSearch()).isNotNull();
        assertThat(req.vectorSearch().tableName()).isEqualTo("cat.sch.vectors");
        assertThat(req.vectorSearch().vectorColumn()).isEqualTo("embedding");
        assertThat(req.vectorSearch().topK()).isEqualTo(10);
        assertThat(req.vectorSearch().distanceMetric()).isEqualTo("L2");
        assertThat(req.vectorSearch().queryVector()).hasSize(3);
    }

    @Test
    void vectorSearchNullWhenNotProvided() {
        SubmitTaskRequest req = new SubmitTaskRequest(
                "t1", "q1", 0, 0, "READ",
                "cGxhbg==", null, null, null, null, null, null,
                null, null, null, null, null, null, null);
        assertThat(req.vectorSearch()).isNull();
    }

    @Test
    void backwardCompatible17ArgConstructor() {
        SubmitTaskRequest req = new SubmitTaskRequest(
                "t1", "q1", 0, 0, "READ",
                "cGxhbg==", null, null, null, null, null, null,
                null, null, "PUSH", "tenant-1", "user-1", null, null);
        assertThat(req.vectorSearch()).isNull();
        assertThat(req.tenantId()).isEqualTo("tenant-1");
    }

    @Test
    void backwardCompatible14ArgConstructor() {
        SubmitTaskRequest req = new SubmitTaskRequest(
                "t1", "q1", 0, 0, "READ",
                "cGxhbg==", null, null, null, null, null, null,
                null, null, null, null, null, null, null);
        assertThat(req.vectorSearch()).isNull();
    }

    @Test
    void jsonRoundTripPreservesVectorSearch() throws Exception {
        VectorSearchInfoPayload vs = samplePayload();
        SubmitTaskRequest original = requestWithVectorSearch(vs);

        String json = MAPPER.writeValueAsString(original);
        SubmitTaskRequest restored = MAPPER.readValue(json, SubmitTaskRequest.class);

        assertThat(restored.vectorSearch()).isNotNull();
        assertThat(restored.vectorSearch().tableName()).isEqualTo("cat.sch.vectors");
        assertThat(restored.vectorSearch().vectorColumn()).isEqualTo("embedding");
        assertThat(restored.vectorSearch().topK()).isEqualTo(10);
        assertThat(restored.vectorSearch().distanceMetric()).isEqualTo("L2");
        assertThat(restored.vectorSearch().queryVector())
                .containsExactly(0.1f, 0.2f, 0.3f);
        assertThat(restored.vectorSearch().indexId()).isEqualTo("idx-hnsw-1");
        assertThat(restored.vectorSearch().searchParams())
                .containsEntry("efSearch", "64");
    }

    @Test
    void jsonRoundTripNullVectorSearch() throws Exception {
        SubmitTaskRequest original = new SubmitTaskRequest(
                "t1", "q1", 0, 0, "READ",
                "cGxhbg==", null, null, null, null, null, null,
                null, null, null, null, null, null, null);
        String json = MAPPER.writeValueAsString(original);
        SubmitTaskRequest restored = MAPPER.readValue(json, SubmitTaskRequest.class);
        assertThat(restored.vectorSearch()).isNull();
    }

    @Test
    void vectorSearchPayloadImmutability() {
        VectorSearchInfoPayload vs = new VectorSearchInfoPayload(
                "t", "col",
                List.of(1.0f, 2.0f), 5, "COSINE", "idx", Map.of("k", "v"),
                2, null);
        org.junit.jupiter.api.Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> vs.queryVector().add(9.9f));
        org.junit.jupiter.api.Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> vs.searchParams().put("x", "y"));
        assertThat(vs.filterPredicate()).isEmpty();
    }

    @Test
    void vectorSearchOversampleAndFilterPredicate() {
        VectorSearchInfoPayload vs = new VectorSearchInfoPayload(
                "t", "col",
                List.of(1.0f), 10, "INNER_PRODUCT", "",
                Map.of(), 3, List.of(new byte[]{0x01, 0x02}));
        assertThat(vs.topKOversample()).isEqualTo(3);
        assertThat(vs.filterPredicate()).hasSize(1);
    }

    // --- Helpers ---

    private static VectorSearchInfoPayload samplePayload() {
        return new VectorSearchInfoPayload(
                "cat.sch.vectors", "embedding",
                List.of(0.1f, 0.2f, 0.3f), 10, "L2", "idx-hnsw-1",
                Map.of("efSearch", "64"), 0, null);
    }

    private static SubmitTaskRequest requestWithVectorSearch(VectorSearchInfoPayload vs) {
        return new SubmitTaskRequest(
                "task-1", "query-1", 0, 0, "READ",
                "cGxhbg==", List.of(), List.of(), null,
                Map.of(), null, null, List.of(), null, "PUSH",
                "tenant-1", "user-1", vs, null);
    }
}
