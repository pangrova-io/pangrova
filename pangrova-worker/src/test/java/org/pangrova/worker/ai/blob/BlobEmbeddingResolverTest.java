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
package org.pangrova.worker.ai.blob;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.worker.ai.embedding.EmbeddingProviderLookup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobEmbeddingResolverTest {

    private final StubBlobEmbeddingProvider stubProvider =
            new StubBlobEmbeddingProvider("clip_vit", 512, 16);

    private final EmbeddingProviderLookup lookup = modelName -> {
        if ("clip_vit".equals(modelName)) {
            return stubProvider;
        }
        return null;
    };

    private final BlobEmbeddingResolver resolver = new BlobEmbeddingResolver(lookup::resolve);

    @Test
    void embedSingleBlobResolvesReferenceAndReturnsEmbedding() {
        Map<String, String> fields = blobFields(
                "s3://bucket/blob/image/abc.jpg", "image/jpeg", "1024", "deadbeef01");

        float[] result = resolver.embedSingleBlob("clip_vit", fields);

        assertThat(result).hasSize(512);
        assertThat(stubProvider.lastEmbeddedRefs()).hasSize(1);
        assertThat(stubProvider.lastEmbeddedRefs().get(0).uri())
                .isEqualTo("s3://bucket/blob/image/abc.jpg");
        assertThat(stubProvider.lastEmbeddedRefs().get(0).mimeType())
                .isEqualTo("image/jpeg");
        assertThat(stubProvider.lastEmbeddedRefs().get(0).size()).isEqualTo(1024L);
        assertThat(stubProvider.lastEmbeddedRefs().get(0).checksum()).isEqualTo("deadbeef01");
    }

    @Test
    void embedSingleBlobThrowsOnUnknownModel() {
        Map<String, String> fields = blobFields(
                "s3://bucket/blob/image/abc.jpg", "image/jpeg", "1024", "deadbeef01");

        assertThatThrownBy(() -> resolver.embedSingleBlob("unknown_model", fields))
                .isInstanceOf(PangrovaException.class)
                .satisfies(ex -> assertThat(((PangrovaException) ex).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE));
    }

    @Test
    void embedSingleBlobThrowsOnMissingUri() {
        Map<String, String> fields = new HashMap<>();
        fields.put(BlobEmbeddingResolver.FIELD_MIME_TYPE, "image/jpeg");
        fields.put(BlobEmbeddingResolver.FIELD_SIZE, "1024");
        fields.put(BlobEmbeddingResolver.FIELD_CHECKSUM, "deadbeef01");

        assertThatThrownBy(() -> resolver.embedSingleBlob("clip_vit", fields))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uri");
    }

    @Test
    void embedSingleBlobThrowsOnMissingMimeType() {
        Map<String, String> fields = new HashMap<>();
        fields.put(BlobEmbeddingResolver.FIELD_URI, "s3://bucket/blob/image/abc.jpg");
        fields.put(BlobEmbeddingResolver.FIELD_SIZE, "1024");
        fields.put(BlobEmbeddingResolver.FIELD_CHECKSUM, "deadbeef01");

        assertThatThrownBy(() -> resolver.embedSingleBlob("clip_vit", fields))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mime_type");
    }

    @Test
    void embedSingleBlobThrowsOnInvalidSize() {
        Map<String, String> fields = blobFields(
                "s3://bucket/blob/image/abc.jpg", "image/jpeg", "not_a_number", "deadbeef01");

        assertThatThrownBy(() -> resolver.embedSingleBlob("clip_vit", fields))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid blob size");
    }

    @Test
    void embedSingleBlobThrowsOnProviderFailure() {
        EmbeddingProviderLookup failingLookup = modelName -> new FailingBlobProvider();
        BlobEmbeddingResolver failingResolver = new BlobEmbeddingResolver(failingLookup::resolve);

        Map<String, String> fields = blobFields(
                "s3://bucket/blob/image/abc.jpg", "image/jpeg", "1024", "deadbeef01");

        assertThatThrownBy(() -> failingResolver.embedSingleBlob("clip_vit", fields))
                .isInstanceOf(PangrovaException.class)
                .satisfies(ex -> assertThat(((PangrovaException) ex).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE));
    }

    @Test
    void embedBlobBatchReturnsEmbeddingsInOrder() {
        List<Map<String, String>> batch = List.of(
                blobFields("s3://bucket/a.jpg", "image/jpeg", "100", "aaa"),
                blobFields("s3://bucket/b.png", "image/png", "200", "bbb"),
                blobFields("s3://bucket/c.pdf", "application/pdf", "300", "ccc")
        );

        List<float[]> results = resolver.embedBlobBatch("clip_vit", batch);

        assertThat(results).hasSize(3);
        assertThat(results.get(0)).hasSize(512);
        assertThat(results.get(1)).hasSize(512);
        assertThat(results.get(2)).hasSize(512);
        assertThat(stubProvider.lastEmbeddedRefs()).hasSize(3);
    }

    @Test
    void embedBlobBatchHandlesNullEntries() {
        List<Map<String, String>> batch = new ArrayList<>();
        batch.add(blobFields("s3://bucket/a.jpg", "image/jpeg", "100", "aaa"));
        batch.add(null);
        batch.add(blobFields("s3://bucket/c.pdf", "application/pdf", "300", "ccc"));

        List<float[]> results = resolver.embedBlobBatch("clip_vit", batch);

        assertThat(results).hasSize(3);
        assertThat(results.get(0)).hasSize(512);
        assertThat(results.get(1)).isNull();
        assertThat(results.get(2)).hasSize(512);
        assertThat(stubProvider.lastEmbeddedRefs()).hasSize(2);
    }

    @Test
    void embedBlobBatchReturnsAllNullsWhenAllEntriesNull() {
        List<Map<String, String>> batch = new ArrayList<>();
        batch.add(null);
        batch.add(null);

        List<float[]> results = resolver.embedBlobBatch("clip_vit", batch);

        assertThat(results).hasSize(2);
        assertThat(results.get(0)).isNull();
        assertThat(results.get(1)).isNull();
    }

    @Test
    void embedBlobBatchReturnsEmptyListForEmptyInput() {
        List<float[]> results = resolver.embedBlobBatch("clip_vit", List.of());

        assertThat(results).isEmpty();
    }

    @Test
    void embedBlobBatchRespectsBatchSize() {
        // Provider maxBatchSize is 16; send 20 blobs to trigger batching
        List<Map<String, String>> batch = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            batch.add(blobFields("s3://bucket/" + i + ".jpg", "image/jpeg",
                    String.valueOf(100 + i), "hash" + i));
        }

        List<float[]> results = resolver.embedBlobBatch("clip_vit", batch);

        assertThat(results).hasSize(20);
        // Provider was called in 2 batches (16 + 4)
        assertThat(stubProvider.embedBlobCallCount()).isEqualTo(2);
    }

    @Test
    void constructorRejectsNullLookup() {
        assertThatThrownBy(() -> new BlobEmbeddingResolver(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("providerLookup");
    }

    @Test
    void embedSingleBlobRejectsNullModelName() {
        Map<String, String> fields = blobFields(
                "s3://bucket/a.jpg", "image/jpeg", "100", "aaa");

        assertThatThrownBy(() -> resolver.embedSingleBlob(null, fields))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void embedSingleBlobRejectsBlankModelName() {
        Map<String, String> fields = blobFields(
                "s3://bucket/a.jpg", "image/jpeg", "100", "aaa");

        assertThatThrownBy(() -> resolver.embedSingleBlob("  ", fields))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("modelName");
    }

    @Test
    void resolveReferenceConstructsCorrectBlobReference() {
        Map<String, String> fields = blobFields(
                "s3://data/blob/img/uuid.jpg", "image/jpeg", "67108864", "sha256hex");

        BlobReference ref = resolver.resolveReference(fields);

        assertThat(ref.uri()).isEqualTo("s3://data/blob/img/uuid.jpg");
        assertThat(ref.mimeType()).isEqualTo("image/jpeg");
        assertThat(ref.size()).isEqualTo(67108864L);
        assertThat(ref.checksum()).isEqualTo("sha256hex");
    }

    // --- Test helpers ---

    private static Map<String, String> blobFields(String uri, String mimeType,
                                                  String size, String checksum) {
        Map<String, String> fields = new HashMap<>();
        fields.put(BlobEmbeddingResolver.FIELD_URI, uri);
        fields.put(BlobEmbeddingResolver.FIELD_MIME_TYPE, mimeType);
        fields.put(BlobEmbeddingResolver.FIELD_SIZE, size);
        fields.put(BlobEmbeddingResolver.FIELD_CHECKSUM, checksum);
        return fields;
    }

    /**
     * Stub provider that records blob references and returns deterministic vectors.
     */
    private static final class StubBlobEmbeddingProvider implements EmbeddingProvider {

        private final String modelName;
        private final int dimensions;
        private final int maxBatchSize;
        private volatile List<BlobReference> lastEmbeddedRefs;
        private volatile int embedBlobCallCount;

        StubBlobEmbeddingProvider(String modelName, int dimensions, int maxBatchSize) {
            this.modelName = modelName;
            this.dimensions = dimensions;
            this.maxBatchSize = maxBatchSize;
        }

        @Override
        public float[] embed(String text) {
            return generateVector(text);
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            return texts.stream().map(this::generateVector).toList();
        }

        @Override
        public List<float[]> embedBlob(List<BlobReference> refs) {
            this.lastEmbeddedRefs = List.copyOf(refs);
            this.embedBlobCallCount++;
            return refs.stream().map(r -> generateVector(r.uri())).toList();
        }

        @Override
        public int dimensions() {
            return dimensions;
        }

        @Override
        public String modelName() {
            return modelName;
        }

        @Override
        public int maxBatchSize() {
            return maxBatchSize;
        }

        List<BlobReference> lastEmbeddedRefs() {
            return lastEmbeddedRefs;
        }

        int embedBlobCallCount() {
            return embedBlobCallCount;
        }

        private float[] generateVector(String input) {
            float[] vec = new float[dimensions];
            int hash = input.hashCode();
            for (int i = 0; i < dimensions; i++) {
                vec[i] = ((hash >> i) & 0xFF) / 255.0f;
            }
            return vec;
        }
    }

    /**
     * Provider that always throws to test error propagation.
     */
    private static final class FailingBlobProvider implements EmbeddingProvider {

        @Override
        public float[] embed(String text) {
            throw new RuntimeException("Connection refused");
        }

        @Override
        public List<float[]> embedBatch(List<String> texts) {
            throw new RuntimeException("Connection refused");
        }

        @Override
        public List<float[]> embedBlob(List<BlobReference> refs) {
            throw new RuntimeException("Connection refused");
        }

        @Override
        public int dimensions() {
            return 512;
        }

        @Override
        public String modelName() {
            return "failing_model";
        }

        @Override
        public int maxBatchSize() {
            return 16;
        }
    }
}
